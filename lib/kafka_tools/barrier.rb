
module KafkaTools
  class TimeoutError < StandardError; end

  class Barrier
    def initialize(zk_hosts:, seed_brokers:, client_id: "kafka_tools", pool_size: 5, pool_timeout: 5, logger: Logger.new("/dev/null"))
      @zk_hosts = zk_hosts

      @kafka_pool = ConnectionPool.new(size: pool_size, timeout: pool_timeout) do
        Kafka.new(seed_brokers: seed_brokers, client_id: client_id)
      end

      @logger = logger
    end

    def wait(topic:, name:, partition: 0, poll_interval: 1, timeout: 30)
      offset = @kafka_pool.with { |kafka| kafka.last_offset_for(topic, partition) }

      start_time = Time.now.to_f

      until zk.get("/kafka_tools/consumer/#{topic}/#{partition}/#{name}/offset")[0].to_i > offset
        if Time.now.to_f - start_time > timeout
          @logger.error "Barrier (#{topic}, #{partition}, #{name}) didn't finish within specified timeout (#{timeout})"

          return false
        end

        sleep poll_interval
      end

      true
    end

    private

    def zk
      @zk ||= ZK.new(@zk_hosts)
    end
  end
end

