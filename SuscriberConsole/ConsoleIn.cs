
using InterfaceLibrarySubscriber;
using InterfaceLibraryConfigurator;
using InterfaceLibraryDeserializer;
using System;
using System.Text;
using System.Collections.Generic;
using System.Threading;
using InterfaceLibraryLogger;
using NLog;
using Definitions;
using System.Threading.Tasks;


namespace SuscriberConsole
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
    public class ConsoleIn : ISubscriber
    {
        private IConfigurator _configurator;
        private Logger _logger;
        private string _id;
        private Dictionary<string, DestinationStruct> _dicDestination = new Dictionary<string, DestinationStruct>();
        private bool _loop = true;
        public void init(string id, IConfigurator configurator, IGenericLogger logger)
        {
            try
            {
                _id = id;
                _logger = (Logger)logger.init("SuscriberConsole");
                _configurator = configurator;

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

                foreach (KeyValuePair<string, DestinationStruct> entry in _dicDestination)
                {
                    string recipient = entry.Key;

                    if (subRecipient is not null)
                    {
                        recipient = recipient + "_" + subRecipient;
                    }
                    Console.WriteLine("Suscripcion en: {0}", recipient);
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
            _logger.Trace("Inicio");
            byte[] data;
            while (_loop)
            {
                try
                {
                    Console.WriteLine("Cola/Topico/Tarea:");
                    string function = Console.ReadLine();
                    Console.WriteLine("Dato:");
                    data = Encoding.UTF8.GetBytes(Console.ReadLine());
                    DestinationStruct currentDeserializer;

                    Dictionary<string,object> metadata = new Dictionary<string, object>();
                    metadata.Add("recipientName", (object)function);
                    if (_dicDestination.TryGetValue(function, out currentDeserializer))
                    {
                        if (currentDeserializer.deserializer.deserialize(data,metadata))
                        {
                            _logger.Error($"No se pudo procesar el mensaje");
                        }
                    }
                    else
                    {
                        _logger.Error($"No se encontro registrada en el modulo la tarea {function}");
                        throw new Exception($"No se encontro registrada en el modulo la tarea {function}");
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error(ex.ToString());
                }
            }
        }
        public void endLoop()
        {
            try
            {
                _logger.Trace("Inicio");
                _loop = false;
                _logger.Trace("Fin");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
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
