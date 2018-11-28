
module KafkaSync
  # @api private
  #
  # The KafkaSync::ThreadStatus is used to signal leadership changes to the
  # as_leader and as_follower blocks passed to a KafkaSync::LeaderElection
  # instance.

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

