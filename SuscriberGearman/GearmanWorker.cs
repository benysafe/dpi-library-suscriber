
using InterfaceLibraryConfigurator;
using InterfaceLibraryDeserializer;
using InterfaceLibrarySubscriber;
using Definitions;
using Twingly.Gearman;
using System;
using System.Collections.Generic;
using System.Threading;
using NLog;
using InterfaceLibraryLogger;
using System.Threading.Tasks;

namespace SuscriberGearman
{
    public class DestinationStruct
    {
        public string recipient = null;
        public string deserializerId = null;
        public IDeserializer deserializer;

        public void SetDeserializer(IDeserializer deserializer)
        {
            this.deserializer = deserializer;
        }
    }
    public class GearmanWorker : ISubscriber
    {
        private IConfigurator _configurator;
        private Logger _logger;
        private string _id;
        private List<GearmanThreadedWorker> _listWorkers = new List<GearmanThreadedWorker>();
        private Dictionary<string, DestinationStruct> _dicDestination = new Dictionary<string, DestinationStruct>();
        string _host;
        int _port;


        public void init(string id, IConfigurator configurator, IGenericLogger logger)
        {
            try
            {
                _configurator = configurator;
                _id = id;
                _logger = (Logger)logger.init("SuscriberGearman");
                object hostName;
                object port;
                if (!configurator.getMap(Config.Subscriptors, _id).TryGetValue(Config.Host, out hostName))
                {
                    _logger.Error($"No se encontro el parametro '{Config.Host}' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro '{Config.Host}' en el suscriptor de id '{_id}'");
                }
                if (!configurator.getMap(Config.Subscriptors, _id).TryGetValue(Config.Port, out port))
                {
                    _logger.Error($"No se encontro el parametro '{Config.Port}' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro '{Config.Port}' en el suscriptor de id '{_id}'");
                }
                _logger.Debug("Worker de Gearman en '{0}:{1}'", hostName.ToString(), port.ToString());
                _host = hostName.ToString();
                _port = Convert.ToInt32(port.ToString());

                object tempValue;
                if (!configurator.getMap(Config.Subscriptors, _id).TryGetValue("mssgBoxes", out tempValue))
                {
                    _logger.Error($"No se encontro el parametro 'mssgBoxes' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro 'mssgBoxes' en el suscriptor de id '{_id}'");
                }
                List<object> listMssgBoxes = System.Text.Json.JsonSerializer.Deserialize<List<object>>(tempValue.ToString());

                if (listMssgBoxes.Count < 1)
                {
                    _logger.Error($"No se encontro ningun parametro en 'mssgBoxes' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro en 'mssgBoxes' en el suscriptor de id '{_id}'");
                }
                for (int index = 0; index < listMssgBoxes.Count; index++)
                {
                    Dictionary<string, string> tempMssgBox = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(listMssgBoxes[index].ToString());
                    DestinationStruct tempDetination = new DestinationStruct();
                    if (!tempMssgBox.TryGetValue("recipient", out tempDetination.recipient))
                    {
                        _logger.Error($"No se encontro el parametro 'recipient' en el MssgBox[{index + 1}] del suscriptor de id '{_id}'");
                        throw new Exception($"No se encontro el parametro 'recipient' en el MssgBox[{index + 1}] del suscriptor de id '{_id}'");
                    }
                    if (!tempMssgBox.TryGetValue("deserializerId", out tempDetination.deserializerId))
                    {
                        _logger.Error($"No se encontro el parametro 'deserializerId' en el MssgBox[{index + 1}] del suscriptor de id '{_id}'");
                        throw new Exception($"No se encontro el parametro 'deserializerId' en el MssgBox[{index + 1}] del suscriptor de id '{_id}'");
                    }
                    this._dicDestination.Add(tempDetination.recipient, tempDetination);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
        public void subscribe(string subRecipient = null)
        {
            try
            {
                _logger.Trace("Inicio");

                object currentThrea;
                if (!_configurator.getMap(Config.Subscriptors, _id).TryGetValue(Config.ConcurrentThread, out currentThrea))
                {
                    _logger.Error($"No se encontro el parametro'{Config.ConcurrentThread}' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro'{Config.ConcurrentThread}' en el suscriptor de id '{_id}'");
                }
                if (Convert.ToInt32(currentThrea.ToString()) <= 0)
                {
                    _logger.Fatal("La cantidada de hilos del modulo no puede ser '{0}'", currentThrea);
                    throw new Exception($"La cantidada de hilos del modulo no puede ser '{currentThrea.ToString()}'");
                }

                for (int i = 0; i < Convert.ToInt32(currentThrea.ToString()); i++)
                {
                    Guid obj = Guid.NewGuid();
                    var worker = new GearmanThreadedWorker();
                    worker.AddServer(_host, _port);
                    worker.SetClientId(obj.ToString());
                    _listWorkers.Add(worker);
                }

                foreach (KeyValuePair<string, DestinationStruct> entry in _dicDestination)
                {
                    string recipient = entry.Key;
                    for (int j = 0; j < _listWorkers.Count; j++)
                    {
                        if (subRecipient is not null)
                        {
                            recipient = recipient + "_" + subRecipient;
                        }
                        _listWorkers[j].RegisterFunction<string, string>(recipient, Function, Serializers.UTF8StringDeserialize, Serializers.UTF8StringSerialize);
                    }
                }
                _logger.Trace("Fin");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
        public void startLoop()
        {
            try
            {
                _logger.Trace("Inicio");
                foreach (GearmanThreadedWorker work in _listWorkers)
                {
                    work.StartWorkLoop();
                }
                _logger.Trace("Fin");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
        public void endLoop()
        {
            try
            {
                _logger.Trace("Inicio");
                foreach (GearmanThreadedWorker work in _listWorkers)
                {
                    work.StopWorkLoop();
                }
                _logger.Trace("Fin");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
            }
        }
        private void Function(IGearmanJob<string, string> job)
        {
            try
            {
                _logger.Trace("Inicio");
                DestinationStruct currentDeserializer;
                if (!this._dicDestination.TryGetValue(job.Info.FunctionName, out currentDeserializer))
                {
                    _logger.Error("No se encontro registrada en el modulo la tarea '{0}'", job.Info.FunctionName);
                    throw new Exception($"No se encontro registrada en el modulo la tarea '{job.Info.FunctionName}'");
                }
                var data = System.Text.Encoding.UTF8.GetBytes(job.FunctionArgument);
                //var data = job.Info.FunctionArgument;
                
                Dictionary<string, object> metadata = new Dictionary<string, object>();
                metadata.Add("recipientName", (object)job.Info.FunctionName);

                if (currentDeserializer.deserializer.deserialize(data, metadata))  //Todo: verificar
                    job.Complete();
                else
                    job.Fail();
                _logger.Trace("Fin");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
            }
        }
        public void addDeserializer(string id, IDeserializer deserializer)
        {
            try
            {
                _logger.Trace("Inicio");
                bool added = false;
                string strRecipent = null;

                Dictionary<string, DestinationStruct> dicDestinationTemp = new Dictionary<string, DestinationStruct>();

                foreach (KeyValuePair<string, DestinationStruct> entry in _dicDestination)
                {
                    if (entry.Value.deserializerId == id)
                    {
                        DestinationStruct tempDestination = new DestinationStruct();
                        strRecipent = entry.Value.recipient;
                        tempDestination.recipient = entry.Value.recipient;
                        tempDestination.deserializerId = entry.Value.deserializerId;
                        tempDestination.deserializer = deserializer;

                        added = true;
                        dicDestinationTemp.Remove(strRecipent);
                        dicDestinationTemp.Add(strRecipent, tempDestination);
                    }
                }
                _dicDestination.Remove(strRecipent);
                _dicDestination = dicDestinationTemp;

                if (!added)
                    _logger.Error($"No se encontro un deserializador con id '{id}' en la configuracion de la suscriptor de id '{_id}'");
                _logger.Trace("Fin");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
    }
}
