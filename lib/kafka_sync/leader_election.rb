
require "securerandom"

module KafkaSync
  # The KafkaSync::LeaderElection class performs a leader election using
  # zookeeper. KafkaSync is using the ruby-zk gem for connecting to zookeeper.
  # Details regarding how to do leader election using zookeeper can be found
  # within the zookeeper docs.
  #
  # @example
  #   leader_election = KafkaSync::LeaderElection.new(
  #     zk: KafkaSync.zk,
  #     path: "/some/path",
  #     value: `hostname`.strip,
  #     logger: Logger.new(STDOUT)
  #   )
  #
  #   leader_election.as_leader do |status|
  #     until status.stopping?
  #       # ...
  #     end
  #   end
  #
  #   leader_election.as_follower do |status|
  #     # ...
  #   end
  #
  #   leader_election.run

  class LeaderElection
    include MonitorMixin

    # Initializes the LeaderElection.
    #
    # @param zk [ZK] A ruby-zk zookeeper connection instance
    # @param path [String] A unique zookeeper path for the leader election
    # @param value [String] Every unique value for this instance participating
    #   in the leader election. Often the host's hostname is used.
    # @param logger [Logger] A logger instance for debug and error messages

    def initialize(zk:, path:, value:, logger: Logger.new("/dev/null"))
      @zk = zk
      @path = path
      @value = value
      @id = SecureRandom.hex
      @logger = logger
      @status = KafkaSync::ThreadStatus.new

      super()
    end

    # Performs the actual leader election, i.e. voting, leader detection and
    # performing either the as_leader or as_follower blocks. Please note that
    # the method is not blocking, such that you probably need to call sleep to
    # avoid process termination.

    def run
      @zk.on_connected { elect }

      elect
    end

    # Specifies a block that will be run in case the leader election is won.
    # The block receives an additional status parameter, which must continously
    # be checked to recognize leadership changes. Please note that you don't
    # have to specify a as_leader block, but it doesn't make much sense to
    # don't specify one. However, if you don't specify a block, nothing will be
    # executed.
    #
    # @param block [Proc] The block to execute when the leader election is won
    #
    # @example
    #   leader_election.as_leader do |status|
    #     until status.stopping?
    #       # ...
    #     end
    #   end

    def as_leader(&block)
      synchronize do
        @leader_proc = block
      end
    end

    # Specifies a block that will be run in case the leader election is lost.
    # The block receives an additional status parameter, which must continously
    # be checked to recognize leadership changes. Please note that you don't
    # have to specify a as_follower block. If no block is specified no block,
    # nothing will be executed.
    #
    # @param block [Proc] The block to execute when the leader election is lost
    #
    # @example
    #   leader_election.as_follower do |status|
    #     until status.stopping?
    #       # ...
    #     end
    #   end

    def as_follower(&block)
      synchronize do
        @follower_proc = block
      end
    end

    private

    def leader?
      synchronize do
        sequence_numbers.first == @sequence_number
      end
    end

    def vote
      synchronize do
        return if @sequence_number && sequence_numbers.include?(@sequence_number)

        @zk.children(@path).select { |node| node.start_with? "#{@id}_" }.each { |node| @zk.delete File.join(@path, node) }

        @sequence_number = node_sequence_number(@zk.create(File.join(@path, "#{@id}_"), @value, mode: :ephemeral_sequential))
      end
    end

    def elect
      @zk.mkdir_p(@path) unless @zk.exists?(@path)

      vote

      if leader?
        become_leader
      else
        become_follower

        predecessor_node = nodes.select { |node| node_sequence_number(node) < @sequence_number }.last

        return elect unless predecessor_node

        synchronize do
          @subscription.unsubscribe if @subscription

          @subscription = @zk.register(File.join(@path, predecessor_node)) do |event|
            begin
              if event.node_deleted? && leader?
                become_leader
              else
                @zk.exists?(File.join(@path, predecessor_node), watch: true) || elect
              end
            rescue => e
              @logger.error e

              sleep 5

              retry
            end
          end
        end

        @zk.exists?(File.join(@path, predecessor_node), watch: true) || elect
      end
    rescue => e
      @logger.error e

      sleep 5

      retry
    end

    def become_leader
      @logger.info "Becoming leader for #{@path}"

      synchronize do
        return if @leader_thread && @leader_thread.alive?

        @follower_thread = nil

        @status.stop
        @status = KafkaSync::ThreadStatus.new

        if @leader_proc
          @leader_thread = Thread.new do
            begin
              @leader_proc.call(@status)
            rescue => e
              @logger.error e
            end
          end
        end
      end
    end

    def become_follower
      synchronize do
        return if @follower_thread && @follower_thread.alive?

        @leader_thread = nil

        @status.stop
        @status = KafkaSync::ThreadStatus.new

        if @follower_proc
          @follower_thread = Thread.new do
            begin
              @follower_proc.call(@status)
            rescue => e
              @logger.error e
            end
          end
        end
      end
    end

    def node_sequence_number(node)
      node.gsub(/.*_/, "").to_i
    end

    def sequence_numbers
      nodes.collect { |node| node_sequence_number node }
    end

    def nodes
      @zk.children(@path).sort_by { |node| node_sequence_number node }
    end
  end
end

