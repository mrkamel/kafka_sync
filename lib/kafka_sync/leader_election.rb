
require "securerandom"

module KafkaSync
  class LeaderElection
    include MonitorMixin

    def initialize(zk:, path:, value:, logger: Logger.new("/dev/null"))
      @zk = zk
      @path = path
      @value = value
      @id = SecureRandom.hex
      @logger = logger
      @status = KafkaSync::ThreadStatus.new

      super()
    end

    def run
      @zk.on_connected { elect }

      elect
    end

    def as_leader(&block)
      synchronize do
        @leader_proc = block
      end
    end

    def as_follower(&block)
      synchronize do
        @follower_proc = block
      end
    end

    def leader?
      synchronize do
        sequence_numbers.first == @sequence_number
      end
    end

    private

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
        @status.stop
        @status = KafkaSync::ThreadStatus.new

        if @leader_proc
          Thread.new do
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
        @status.stop
        @status = KafkaSync::ThreadStatus.new

        if @follower_proc
          Thread.new do
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

