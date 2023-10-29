using InterfaceLibraryConfigurator;
using InterfaceLibraryDeserializer;
using InterfaceLibrarySubscriber;
using Definitions;
using System;
using System.Collections.Generic;
using System.Threading;
using NLog;
using InterfaceLibraryLogger;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.Text;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Avro.Generic;
using Avro;
using Confluent.Kafka.SyncOverAsync;

namespace SuscriberKafka
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
    public class Kafka : ISubscriber
    {
        private IConfigurator _configurator;
        private Logger _logger;
        private string _id;
        private Dictionary<string, DestinationStruct> _dicDestination = new Dictionary<string, DestinationStruct>();
        private bool bLoop = true;

        private ConsumerConfig _consumerConfig;
        private string _typeSchema = null;
        private IConsumer<Ignore, GenericRecord> _consumerAvro;
        private IConsumer<Ignore, string> _consumer;
        private bool _bConteinSchema = false;
        private string _schemaRegistryList = null;


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
                if(_bConteinSchema)
                {
                    if(_typeSchema =="avro")
                    {
                        _consumerAvro.Unsubscribe();
                    }
                }
                else
                {
                    _consumer.Unsubscribe();
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
                _logger = (Logger)logger.init("SuscriberKafka");
                object bootstrapServers;
                object schemaRegistryUrls;
                object schemaType;
                object groupId;
                object autoCommit;
                object autoOffsetStore;                
                object maxPollIntervalMs;
                object enumAutoOffsetReset;
                object topicName;

                if (!configurator.getMap(Definitions.Config.Subscriptors, _id).TryGetValue("bootstrapServers", out bootstrapServers))
                {
                    _logger.Error($"No se encontro el parametro 'bootstrapServers' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro 'bootstrapServers' en el suscriptor de id '{_id}'");
                }

                if (configurator.getMap(Definitions.Config.Subscriptors, _id).ContainsKey("schema"))
                {
                    if (!configurator.getMap(Definitions.Config.Subscriptors, _id).TryGetValue("schemaRegistryUrls", out schemaRegistryUrls))
                    {
                        _logger.Error($"No se encontro el parametro 'schemaRegistryUrls' en el suscriptor de id '{_id}'");
                        throw new Exception($"No se encontro el parametro 'schemaRegistryUrls' en el suscriptor de id '{_id}'");
                    }
                    if (!configurator.getMap(Definitions.Config.Subscriptors, _id).TryGetValue("schemaType", out schemaType))
                    {
                        _logger.Error($"No se encontro el parametro 'schemaType' en el suscriptor de id '{_id}'");
                        throw new Exception($"No se encontro el parametro 'schemaType' en el suscriptor de id '{_id}'");
                    }
                    _bConteinSchema = true;
                    _schemaRegistryList = schemaRegistryUrls.ToString();
                    _typeSchema = schemaType.ToString();
                }

                if (!configurator.getMap(Definitions.Config.Subscriptors, _id).TryGetValue("groupId", out groupId))
                {
                    _logger.Error($"No se encontro el parametro 'groupId' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro 'groupId' en el suscriptor de id '{_id}'");
                }
                if (!configurator.getMap(Definitions.Config.Subscriptors, _id).TryGetValue("autoCommit", out autoCommit))
                {
                    _logger.Error($"No se encontro el parametro 'autoCommit' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro 'autoCommit' en el suscriptor de id '{_id}'");
                }

                if (!configurator.getMap(Definitions.Config.Subscriptors, _id).TryGetValue("autoOffsetStore", out autoOffsetStore))
                {
                    _logger.Error($"No se encontro el parametro 'autoOffsetStore' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro 'autoOffsetStore' en el suscriptor de id '{_id}'");
                }
                if (!configurator.getMap(Definitions.Config.Subscriptors, _id).TryGetValue("maxPollIntervalMs", out maxPollIntervalMs))
                {
                    _logger.Error($"No se encontro el parametro 'maxPollIntervalMs' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro 'maxPollIntervalMs' en el suscriptor de id '{_id}'");
                }

                if (!configurator.getMap(Definitions.Config.Subscriptors, _id).TryGetValue("autoOffsetReset", out enumAutoOffsetReset))
                {
                    _logger.Error($"No se encontro el parametro 'autoOffsetReset' en el suscriptor de id '{_id}'");
                    throw new Exception($"No se encontro el parametro 'autoOffsetReset' en el suscriptor de id '{_id}'");
                }

                //ToDo: Define how to integrate 'schemaRegistryUrls' to the consumer kafka

                AutoOffsetReset autoOffsetReset = new AutoOffsetReset();
                if (enumAutoOffsetReset.ToString() == "Latest" || enumAutoOffsetReset.ToString() == "latest")
                    autoOffsetReset = AutoOffsetReset.Latest;
                else if (enumAutoOffsetReset.ToString() == "Earliest" || enumAutoOffsetReset.ToString() == "earliest")
                    autoOffsetReset = AutoOffsetReset.Earliest;
                else if (enumAutoOffsetReset.ToString() == "Error" || enumAutoOffsetReset.ToString() == "error")
                    autoOffsetReset = AutoOffsetReset.Error;

                _consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers.ToString(),
                    EnableAutoCommit = (bool)autoCommit,
                    EnableAutoOffsetStore = (bool)autoOffsetStore,
                    MaxPollIntervalMs = (int)maxPollIntervalMs,
                    GroupId = groupId.ToString(),

                    // Read messages from start if no commit exists.
                    AutoOffsetReset = autoOffsetReset
                };

                object tempValue;
                if (!configurator.getMap(Definitions.Config.Subscriptors, _id).TryGetValue("mssgBoxes", out tempValue))
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
                while (bLoop)
                {
                    DestinationStruct currentDeserializer;
                    Dictionary<string, object> metadata = new Dictionary<string, object>();

                    if (_bConteinSchema)    //Dato de tipo Avro basado en esquema
                    {
                        if (_typeSchema == "avro")
                        {
                            var result = _consumerAvro.Consume(TimeSpan.FromMilliseconds(_consumerConfig.MaxPollIntervalMs - 1000 ?? 250000));
                            var message = result?.Message?.Value;
                            if (message == null)
                            {
                                continue;
                            }
                            string sTopic = result?.Topic.ToString();
                            metadata.Add("recipientName", (object)sTopic);
                            metadata.Add("kafkaPartition", (object)result.Partition.Value);

                            if (_consumerConfig.EnableAutoCommit == false)
                                _consumerAvro.Commit(result);
                            if (_consumerConfig.EnableAutoOffsetStore == false)
                                _consumerAvro.StoreOffset(result);

                            if (_dicDestination.TryGetValue(sTopic, out currentDeserializer))
                            {
                                if (currentDeserializer.deserializer.deserialize(message, metadata))
                                {
                                    _logger.Error($"No se pudo procesar el mensaje");
                                }
                            }
                            else
                            {
                                _logger.Error($"No se encontro registrada la tarea '{sTopic}' en el modulo ");
                            }
                        }
                    }
                    else //dato de tipo Json contenido como cadena, sin soporte de esquema
                    {
                        var result = _consumer.Consume(TimeSpan.FromMilliseconds(_consumerConfig.MaxPollIntervalMs - 1000 ?? 250000));
                        var message = result?.Message?.Value;
                        if (message == null)
                        {
                            continue;
                        }
                        string sTopic = result?.Topic.ToString();
                        metadata.Add("recipientName", (object)sTopic);
                        metadata.Add("kafkaPartition", (object)result.Partition.Value);

                        if (_consumerConfig.EnableAutoCommit == false)
                            _consumer.Commit(result);
                        if (_consumerConfig.EnableAutoOffsetStore == false)
                            _consumer.StoreOffset(result);

                        byte[] data = Encoding.UTF8.GetBytes(message);

                        if (_dicDestination.TryGetValue(sTopic, out currentDeserializer))
                        {
                            if (currentDeserializer.deserializer.deserialize(data, metadata))
                            {
                                _logger.Error($"No se pudo procesar el mensaje");
                            }
                        }
                        else
                        {
                            _logger.Error($"No se encontro registrada la tarea '{sTopic}' en el modulo ");
                        }
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

        public void subscribe(string subRecipient = null)
        {
            try
            {
                _logger.Trace("Inicio");
                List<string> lTopicsNames = new List<string>();

                foreach (KeyValuePair<string, DestinationStruct> entry in _dicDestination)
                {
                    string recipient = entry.Key;
                    if (subRecipient is not null)
                    {
                        recipient = recipient + "_" + subRecipient;
                    }
                    lTopicsNames.Add(recipient);
                }
                if(_bConteinSchema)
                {
                    if (_typeSchema == "avro")
                    {
                        var schemaRegistryConfig = new SchemaRegistryConfig
                        {
                            Url = _schemaRegistryList
                        };
                        var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
                        ConsumerBuilder<Ignore, GenericRecord> consumer = new ConsumerBuilder<Ignore, GenericRecord>(_consumerConfig);
                        consumer.SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync());
                        consumer.SetErrorHandler((_, e) => _logger.Error($" {e.Reason}"));
                        _consumerAvro = consumer.Build();
                        _consumerAvro.Subscribe(lTopicsNames);
                    }
                }
                else
                {
                    ConsumerBuilder<Ignore, string> consumer = new ConsumerBuilder<Ignore, string>(_consumerConfig);
                    consumer.SetValueDeserializer(Deserializers.Utf8);
                    consumer.SetErrorHandler((_, e) => _logger.Error($" {e.Reason}"));
                    _consumer = consumer.Build();

                    _consumer.Subscribe(lTopicsNames);
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