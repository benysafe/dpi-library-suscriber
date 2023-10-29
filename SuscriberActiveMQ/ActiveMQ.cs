using InterfaceLibraryConfigurator;
using InterfaceLibraryDeserializer;
using InterfaceLibraryLogger;
using InterfaceLibrarySubscriber;
using Definitions;
using Apache.NMS;
using Apache.NMS.Util;
using NLog;
using System.Text;
using Apache.NMS.ActiveMQ.Commands;

namespace SuscriberActiveMQ
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
    public class ActiveMQQueue : ISubscriber
    {
        private IConfigurator _configurator;
        private Logger _logger;
        private string _id;
        private Dictionary<string, DestinationStruct> _dicDestination = new Dictionary<string, DestinationStruct>();
        private bool bLoop = true;

        private IConnection connection;
        private List<ISession> sessions = new List<ISession>();

        private bool _toQueue;
        private ITextMessage message = null;
        private List<IMessageConsumer> consumers = new List<IMessageConsumer>();


        public void init(string id, IConfigurator configurator, IGenericLogger logger)
        {
            try
            {
                _configurator = configurator;
                _id = id;
                _logger = (Logger)logger.init("SuscriberActiveMQ");
                object strUri;
                if (!configurator.getMap(Config.Subscriptors, _id).TryGetValue(Config.Uri, out strUri))
                {
                    _logger.Error($"No se encontro el parametro'{Config.Uri}' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro'{Config.Uri}' en el suscriptor de id '{_id}'");
                }
                object strTypeOut;
                if (!_configurator.getMap(Config.Subscriptors, _id).TryGetValue("mssgBoxType", out strTypeOut))
                {
                    _logger.Error($"No se encontro el parametro 'mssgBoxType' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro 'mssgBoxType' en el suscriptor de id '{_id}'");
                }
                string strType = strTypeOut.ToString();
                if (strType == "queue")
                {
                    _toQueue = true;
                }
                else if (strType == "topic")
                {
                    _toQueue = false;
                }
                _logger.Debug("Servidor ActiveMQ en '{0}' susctrito a '{1}'", strUri.ToString(), strType);

                Uri connecturi = new Uri(strUri.ToString());
                IConnectionFactory factory = new Apache.NMS.ActiveMQ.ConnectionFactory(connecturi);

                connection = factory.CreateConnection();

                connection.Start();

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

        public void startLoop()
        {
            try
            {
                _logger.Trace("Inicio");

                for (int index = 0; index < consumers.Count; index++)
                {
                    consumers[index].Listener += new MessageListener(OnMessage);
                }
                _logger.Trace("Fin");
                while(bLoop)
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
                    ISession session = connection.CreateSession();
                    IDestination destination;
                    if (_toQueue)
                        destination = session.GetQueue(recipient);
                    else
                        destination = session.GetTopic(recipient);

                    IMessageConsumer consumer = session.CreateConsumer(destination);
                    sessions.Add(session);
                    consumers.Add(consumer);
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
                for (int index = 0; index < sessions.Count; index++)
                {
                    sessions[index].Close();
                }
                connection.Stop();
                connection.Close();
                bLoop = false;
                _logger.Trace("Fin");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
        private void OnMessage(IMessage receivedMsg)
        {
            try
            {
                _logger.Trace("Inicio");
                message = receivedMsg as ITextMessage;
                ActiveMQMessage amqMsg = receivedMsg as ActiveMQMessage;

                DestinationStruct currentDeserializer = new DestinationStruct();

                if (!this._dicDestination.TryGetValue(amqMsg.Destination.PhysicalName, out currentDeserializer))
                {
                    _logger.Error("No se encontro registrada en el modulo la tarea '{0}'", amqMsg.Destination.PhysicalName);
                    throw new Exception($"No se encontro registrada en el modulo la tarea '{amqMsg.Destination.PhysicalName}'");
                }
                Dictionary<string, object> metadata = new Dictionary<string, object>();
                metadata.Add("recipientName", (object)amqMsg.Destination.PhysicalName);
                var data = System.Text.Encoding.UTF8.GetBytes(message.Text);

                currentDeserializer.deserializer.deserialize(data, metadata);

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