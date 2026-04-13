############
ctrl_filterd
############

``ctrl_filterd`` is a daemon that receives Kafka messages from the Rucio Hermes daemon, augments them, and forwards them to ``ctrl_ingestd`` daemons.

The daemon filters all messages it receives, looking for ``transfer-done`` status, which indicates that Rucio has fully transferred a file.  It then looks at the Rucio file, retrieves any metadata associated with it, add it to the Hermes messages, and then forwards it to a Kafka topic. The Kafka topic it uses is specified in the Hermes message under ``dst-rse``.


The ``ctrl_filterd`` daemon is configured via YAML.  The YAML file ``ctrl_filterd`` uses is specified by setting the environment variable ``CTRL_FILTERD_CONFIG``.

Example YAML file
-----------------

.. code-block:: yaml

    brokers: 
        - kafka:9092
    consumer_client_id: filterd_consumer
    producer_client_id: filterd_producer
    group_id: filterd_groupid
    num_messages: 50
    timeout: 1
    topic: hermestopic
    rse_topics: 
         - XRD1-test
         - XRD2-test
         - XRD3-test
         - XRD4-test

The ``brokers`` section is the list of Kafka brokers.

The ``consumer_client_id`` is the Kafka consumer client id.  Defaults to "filterd_consumer", if not specified

The ``producer_client_id`` is the Kafka producer client id.  Defaults to "filterd_producer", if not specified

The ``group_id`` is the consumer group id.  Defaults to "filterd_groupid", if not specified

The ``num_messages`` value is the number of incoming Kafka messages to handle at a time.  Defaults to 50, if not specified

The ``timeout`` the amount of time in seconds to wait for incoming messages before returning and attempting do do work.  Defaults to one second, if not specified

The ``topic`` value is the Kafka topic on which to expect incoming Hermes Kafka messages.  This should be the same Kafka topic as is set for Hermes.

The ``rse_topics`` section is the list of Kafka topics that ``ctrl_filterd`` is expected to send to ``ctrl_ingestd`` daemons at destination RSEs.  These topics are the names of the destination RSEs used in Rubin's deployment of Rucio.

Security & Rucio settings
-------------------------
This container uses the package ``rucio-clients`` and should be configured to be used with the appropriate security settings as the other Rucio containers, and the common ``rucio.cfg`` used by the other Rucio containers.

Configuration with HermesK
--------------------------
If you're running this with HermesK, remove the `message_filter` section, and change `topic_list` to the Kafka topic on which you wish to broadcast Hermes events.

Example:

.. code-block:: text

   [messaging-hermes-kafka]
   nonssl_port = 9092
   use_ssl = False
   brokers = kafka
   topic_list = hermestopic

Logging
-------
The environment variable ``CTRL_FILTERD_LOG_LEVEL`` can be used to switch the debug level.  Default is INFO
