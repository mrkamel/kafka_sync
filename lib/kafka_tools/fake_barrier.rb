
module KafkaTools
  class FakeBarrier
    def initialize(*args)
      # nothing
    end

    def wait(topic:, name:, partition: 0, poll_interval: 1, timeout: 30)
      true
    end
  end
end

