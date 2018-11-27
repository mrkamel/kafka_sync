
require File.expand_path("../../spec_helper", __FILE__)

RSpec.describe KafkaSync::LeaderElection do
  it "should become initial leader" do
    leader = Concurrent::AtomicBoolean.new(false)

    leader_election = KafkaSync::LeaderElection.new(zk: KafkaSync.zk, path: "/leader_election/#{SecureRandom.hex}", value: SecureRandom.hex)
    leader_election.as_leader { |status| leader.make_true }
    leader_election.run

    sleep 1

    expect(leader.value).to eq(true)
  end

  it "should become follower" do
    path = "/leader_election/#{SecureRandom.hex}"

    KafkaSync::LeaderElection.new(zk: KafkaSync.zk, path: path, value: SecureRandom.hex).run

    follower = Concurrent::AtomicBoolean.new(false)

    leader_election = KafkaSync::LeaderElection.new(
      zk: KafkaSync.zk,
      path: path,
      value: SecureRandom.hex
    )

    leader_election.as_follower { |status| follower.make_true }
    leader_election.run

    expect(follower.value).to eq(true)
  end

  it "should become leader when the current leader dies" do
    path = "/leader_election/#{SecureRandom.hex}"

    pid = fork do
      KafkaSync::LeaderElection.new(zk: KafkaSync.zk.tap(&:reopen), path: path, value: SecureRandom.hex).run

      sleep
    end

    sleep 1

    role = Concurrent::AtomicReference.new

    leader_election = KafkaSync::LeaderElection.new(zk: KafkaSync.zk, path: path, value: SecureRandom.hex)
    leader_election.as_leader { |status| role.set(:leader) }
    leader_election.as_follower { |status| role.set(:follower) }
    leader_election.run

    sleep 1

    expect(role.value).to eq(:follower)

    Process.kill("QUIT", pid)

    sleep 1

    expect(role.value).to eq(:leader)
  end
end

