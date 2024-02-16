using InterfaceLibraryConfigurator;
using InterfaceLibraryDeserializer;
using InterfaceLibraryLogger;
using InterfaceLibrarySubscriber;
using Definitions;
using NLog;
using Amqp;
using Amqp.Framing;
using System;
using System.Collections.Generic;
using Amqp.Listener;
using System.Net;
using Amqp.Types;

namespace SuscriberAMQP
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

    public class ConsumerAMQP : ISubscriber
    {
        private IConfigurator _configurator;
        private Logger _logger;
        private string _id;
        private Dictionary<string, DestinationStruct> _dicDestination = new Dictionary<string, DestinationStruct>();
        private bool bLoop = true;

        private string sPort;
        private string sHost;
        private string sessionId;

        Connection host;
        Session session;
        List<ReceiverLink> lReceivers;
        string sGuid = Guid.NewGuid().ToString();


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

        public void endLoop()
        {
            try
            {
                _logger.Trace("Inicio");

                bLoop = false;

                foreach (ReceiverLink item in lReceivers)
                {
                    item.Close();
                }
                session.Close();
                host.Close();
                lReceivers.Clear();
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }

        public void init(string id, IConfigurator configurator, IGenericLogger logger)
        {
            try
            {
                _configurator = configurator;
                _id = id;
                _logger = (Logger)logger.init("SuscriberAMQP");
                object value;
                if (!configurator.getMap(Config.Subscriptors, _id).TryGetValue(Config.Host, out value))
                {
                    _logger.Error($"No se encontro el parametro'{Config.Host}' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro'{Config.Host}' en el suscriptor de id '{_id}'");
                }
                sHost = value.ToString();

                if (!_configurator.getMap(Config.Subscriptors, _id).TryGetValue("port", out value))
                {
                    _logger.Error($"No se encontro el parametro 'port' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro 'port' en el suscriptor de id '{_id}'");
                }
                sPort = value.ToString();

                if (!_configurator.getMap(Config.Subscriptors, _id).TryGetValue("sessionId", out value))
                {
                    _logger.Error($"No se encontro el parametro 'sessionId' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro 'sessionId' en el suscriptor de id '{_id}'");
                }
                sessionId = value.ToString();
                
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

                // Dirección del servidor AMQP
                Address address = new Address("amqp://" + sHost + ":" + sPort);
                // Crear un host y registrar un transportador de transporte personalizado
                host = new Connection(address);
                session = new Session(host);

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

                foreach (ReceiverLink receiver in lReceivers)
                {
                    receiver.Start(1, async (link, message) =>
                    {
                        DestinationStruct currentDeserializer = new DestinationStruct();

                        string sQueue = link.Name;

                        if (!this._dicDestination.TryGetValue(sQueue, out currentDeserializer))
                        {
                            _logger.Error("No se encontro registrada en el modulo la tarea '{0}'", sQueue);
                            throw new Exception($"No se encontro registrada en el modulo la tarea '{sQueue}'");
                        }
                        Dictionary<string, object> metadata = new Dictionary<string, object>();
                        metadata.Add("recipientName", (object)sQueue);
                        var bytes = System.Text.Encoding.UTF8.GetBytes(message.Body.ToString());

                        currentDeserializer.deserializer.deserialize(bytes, metadata);
                    });
                }
                _logger.Trace("Fin");
                while (bLoop)
                {
                    Thread.Sleep(10000);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }

        public void subscribe(string subRecipent = null)
        {
            try
            {
                _logger.Trace("Inicio");

                foreach (KeyValuePair<string, DestinationStruct> entry in _dicDestination)
                {
                    string recipient = entry.Key;

                    if (subRecipent is not null)
                    {
                        recipient = recipient + "_" + subRecipent;
                    }

                    var receiver = new ReceiverLink(session, sessionId, recipient);
                    lReceivers.Add(receiver);
                }
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
