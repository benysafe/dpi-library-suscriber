using InterfaceLibraryConfigurator;
using InterfaceLibraryDeserializer;
using InterfaceLibraryLogger;
using InterfaceLibrarySubscriber;
using Definitions;
using NLog;
using System.Text;
using System.Security.Cryptography;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Extensions.ManagedClient;
using System;
using System.Reflection;

namespace SuscriberMQTT
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
    public class ConsumerMQTT : ISubscriber
    {

        private IConfigurator _configurator;
        private Logger _logger;
        private string _id;
        private Dictionary<string, DestinationStruct> _dicDestination = new Dictionary<string, DestinationStruct>();
        private bool bLoop = true;

        private string sProtocolVersion = "3.1.1";
        private string sHost;
        private string sPort;

        IManagedMqttClient mqttClient = new MqttFactory().CreateManagedMqttClient();

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

                var toDisconnect = mqttClient.StopAsync();
                if (!toDisconnect.Wait(TimeSpan.FromSeconds(30)))   //30 segundos para suscribirse al topico en el broker
                {
                    throw new Exception($"30 segundos de tiempo de espera para la desconexion broker 'mqtt://{sHost}:{sPort}/' agotados");
                }
                _logger.Trace("Fin");
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
                _logger = (Logger)logger.init("SuscriberMQTT");
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

                if (_configurator.getMap(Config.Subscriptors, _id).TryGetValue("version", out value))
                {
                    sProtocolVersion = value.ToString();
                }

                // Create client options object
                MqttClientOptionsBuilder builder;
                ;
                if (sProtocolVersion == "5.0")  //MQTTv5.0
                {
                    builder = new MqttClientOptionsBuilder()
                         .WithClientId(Guid.NewGuid().ToString())
                         .WithTcpServer(sHost, Convert.ToInt32(sPort))
                         .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500);
                }
                else if (sProtocolVersion == "3.1.0")   //MQTTv3.1.0
                {
                    builder = new MqttClientOptionsBuilder()
                        .WithClientId(Guid.NewGuid().ToString())
                        .WithTcpServer(sHost, Convert.ToInt32(sPort))
                        .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V310)
                        .WithCleanSession();
                }
                else    //MQTTv3.1.1
                {
                    builder = new MqttClientOptionsBuilder()
                        .WithClientId(Guid.NewGuid().ToString())
                        .WithTcpServer(sHost, Convert.ToInt32(sPort))
                        .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V311)
                        .WithCleanSession();
                }

                ManagedMqttClientOptions options = new ManagedMqttClientOptionsBuilder()
                       .WithAutoReconnectDelay(TimeSpan.FromSeconds(5))         //Autoreconectar con el servidor ante fallas a los 5 segundos
                       .WithClientOptions(builder.Build())
                       .Build();


                var connectResult = mqttClient.StartAsync(options);
                if (!connectResult.Wait(TimeSpan.FromSeconds(30)))   //30 segundos para conectarse al servidor
                {
                    _logger.Error($"30 segundos de tiempo de espera para conexion con el broker 'mqtt://{sHost}:{sPort}/' agotados");
                    throw new Exception($"30 segundos de tiempo de espera para conexion con el broker 'mqtt://{sHost}:{sPort}/' agotados");
                }

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

                mqttClient.ApplicationMessageReceivedAsync += e =>
                {
                    DestinationStruct currentDeserializer = new DestinationStruct();

                    string sTopic = e.ApplicationMessage.Topic;

                    if (!this._dicDestination.TryGetValue(sTopic, out currentDeserializer))
                    {
                        _logger.Error("No se encontro registrada en el modulo la tarea '{0}'", sTopic);
                        throw new Exception($"No se encontro registrada en el modulo la tarea '{sTopic}'");
                    }
                    Dictionary<string, object> metadata = new Dictionary<string, object>();
                    metadata.Add("recipientName", (object)sTopic);
                    var data = System.Text.Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
                    var bytes = System.Text.Encoding.UTF8.GetBytes(data);

                    currentDeserializer.deserializer.deserialize(bytes, metadata);

                    return Task.CompletedTask;
                };

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

                    var toSuscribe = mqttClient.SubscribeAsync(recipient);
                    if (!toSuscribe.Wait(TimeSpan.FromSeconds(30)))   //30 segundos para suscribirse al topico en el broker
                    {
                        throw new Exception($"30 segundos de tiempo de espera para suscribirse al 'topico{recipient}' en el broker 'mqtt://{sHost}:{sPort}/' agotados");
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
        
    }
}