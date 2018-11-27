
module KafkaSync
  class ThreadStatus
    include MonitorMixin

    def initialize
      @stopping = false

      super()
    end

    def stop
      synchronize do
        @stopping = true
      end
    end

    def stopping?
      synchronize do
        @stopping
      end
    end
  end
end

