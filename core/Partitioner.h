//
// Created by Yi Lu on 8/31/18.
//

#pragma once

#include <glog/logging.h>
#include <memory>
#include <numeric>
#include <string>
#define ALL_GRANULES ((1 << 12) - 1)
namespace star
{

class Partitioner {
    public:
	Partitioner(std::size_t coordinator_id, std::size_t coordinator_num)
	{
		DCHECK(coordinator_id < coordinator_num);
		this->coordinator_id = coordinator_id;
		this->coordinator_num = coordinator_num;
	}

	virtual ~Partitioner() = default;

	std::size_t total_coordinators() const
	{
		return coordinator_num;
	}

	virtual std::size_t replica_num() const = 0;

	virtual bool is_replicated() const = 0;

	std::size_t get_coordinator_id()
	{
		return coordinator_id;
	}

	virtual bool has_master_partition(std::size_t partition_id) const = 0;

	virtual std::size_t num_coordinator_for_one_replica() const
	{
		return total_coordinators();
	}

	virtual std::size_t master_coordinator(std::size_t partition_id) const = 0;

	virtual std::size_t get_ith_replica_coordinator(std::size_t partition_id, std::size_t ith) const
	{
		return 0;
	}

	virtual std::vector<std::size_t> get_replica_coordinators(std::size_t partition_id) const
	{
		return {};
	}

	virtual bool is_partition_replicated_on(std::size_t partition_id, std::size_t coordinator_id) const = 0;

	bool is_partition_replicated_on_me(std::size_t partition_id) const
	{
		return is_partition_replicated_on(partition_id, coordinator_id);
	}

	virtual bool is_backup() const = 0;

    protected:
	std::size_t coordinator_id;
	std::size_t coordinator_num;
};

/*
 * N is the total number of replicas.
 * N is always larger than 0.
 * The N coordinators from the master coordinator have the replication for a
 * given partition.
 */

template <std::size_t N> class HashReplicatedPartitioner : public Partitioner {
    public:
	HashReplicatedPartitioner(std::size_t coordinator_id, std::size_t coordinator_num)
		: Partitioner(coordinator_id, coordinator_num)
	{
		CHECK(N > 0 && N <= coordinator_num);
	}

	~HashReplicatedPartitioner() override = default;

	std::size_t replica_num() const override
	{
		return N;
	}

	bool is_replicated() const override
	{
		return N > 1;
	}

	bool has_master_partition(std::size_t partition_id) const override
	{
		return master_coordinator(partition_id) == coordinator_id;
	}

	std::size_t master_coordinator(std::size_t partition_id) const override
	{
		return partition_id % coordinator_num;
	}

	std::size_t get_ith_replica_coordinator(std::size_t partition_id, std::size_t ith) const override
	{
		DCHECK(ith < replica_num());
		std::size_t first_replica = master_coordinator(partition_id);
		if (ith == 0)
			return first_replica;
		std::size_t last_replica = (first_replica + N - 1) % coordinator_num;
		if (last_replica >= first_replica) {
			return first_replica + ith;
		} else {
			return (first_replica + ith) % coordinator_num;
			// if (first_replica + ith < coordinator_num)
			//   return first_replica + ith;
			// else
			//   return 0 + coordinator_num - first_replica - ith;
		}
	}

	std::vector<std::size_t> get_replica_coordinators(std::size_t partition_id) const override
	{
		std::size_t first_replica = master_coordinator(partition_id);
		std::size_t last_replica = (first_replica + N - 1) % coordinator_num;
		std::vector<std::size_t> res;
		if (last_replica >= first_replica) {
			for (std::size_t i = first_replica; i <= last_replica; ++i) {
				res.push_back(i);
			}
			return res;
		} else {
			for (std::size_t i = first_replica; i < coordinator_num; ++i) {
				res.push_back(i);
			}
			for (std::size_t i = 0; i <= last_replica; ++i) {
				res.push_back(i);
			}
		}
		DCHECK(res.size() == replica_num());
		return res;
	}

	bool is_partition_replicated_on(std::size_t partition_id, std::size_t coordinator_id) const override
	{
		DCHECK(coordinator_id < coordinator_num);
		std::size_t first_replica = master_coordinator(partition_id);
		std::size_t last_replica = (first_replica + N - 1) % coordinator_num;

		if (last_replica >= first_replica) {
			return first_replica <= coordinator_id && coordinator_id <= last_replica;
		} else {
			return coordinator_id >= first_replica || coordinator_id <= last_replica;
		}
	}

	bool is_backup() const override
	{
		return false;
	}
};

class HStorePBPartitioner : public Partitioner {
    public:
	HStorePBPartitioner(std::size_t coordinator_id, std::size_t coordinator_num)
		: Partitioner(coordinator_id, coordinator_num)
	{
		CHECK(total_coordinators() % 2 == 0);
		DCHECK(total_coordinators() > 1);
	}

	size_t current_replica() const
	{
		return coordinator_id < num_coordinator_for_one_replica() ? 0 : 1;
	}

	std::size_t num_coordinator_for_one_replica() const override
	{
		return total_coordinators() / 2;
	}

	~HStorePBPartitioner() override = default;

	std::size_t replica_num() const override
	{
		return 2;
	}

	bool is_replicated() const override
	{
		return true;
	}

	bool has_master_partition(std::size_t partition_id) const override
	{
		CHECK(false);
	}

	std::size_t master_coordinator(std::size_t partition_id) const override
	{
		CHECK(false);
	}

	std::size_t get_ith_replica_coordinator(std::size_t partition_id, std::size_t ith) const override
	{
		DCHECK(ith < replica_num());
		return partition_id % num_coordinator_for_one_replica() + ith * num_coordinator_for_one_replica();
		// std::size_t first_replica = master_coordinator(partition_id);
		// if (ith == 0)
		//   return first_replica;
		// std::size_t last_replica = (first_replica + N - 1) % coordinator_num;
		// if (last_replica >= first_replica) {
		//   return first_replica + ith;
		// } else {
		//   return (first_replica + ith) % coordinator_num;
		//   // if (first_replica + ith < coordinator_num)
		//   //   return first_replica + ith;
		//   // else
		//   //   return 0 + coordinator_num - first_replica - ith;
		// }
	}

	std::vector<std::size_t> get_replica_coordinators(std::size_t partition_id) const override
	{
		CHECK(false);
		return {};
	}

	bool is_partition_replicated_on(std::size_t partition_id, std::size_t coordinator_id) const override
	{
		DCHECK(coordinator_id < coordinator_num);
		return partition_id % num_coordinator_for_one_replica() == coordinator_id || // either on primary
		       partition_id % num_coordinator_for_one_replica() + num_coordinator_for_one_replica() == coordinator_id; // or on secondary
	}

	bool is_backup() const override
	{
		return false;
	}
};

using HashPartitioner = HashReplicatedPartitioner<1>;

class PrimaryBackupPartitioner : public Partitioner {
    public:
	PrimaryBackupPartitioner(std::size_t coordinator_id, std::size_t coordinator_num)
		: Partitioner(coordinator_id, coordinator_num)
	{
		CHECK(coordinator_num == 2);
	}

	~PrimaryBackupPartitioner() override = default;

	std::size_t replica_num() const override
	{
		return 2;
	}

	bool is_replicated() const override
	{
		return true;
	}

	bool has_master_partition(std::size_t partition_id) const override
	{
		return coordinator_id == 0;
	}

	std::vector<std::size_t> get_replica_coordinators(std::size_t partition_id) const override
	{
		return { 0, 1 };
	}

	std::size_t master_coordinator(std::size_t partition_id) const override
	{
		return 0;
	}

	bool is_partition_replicated_on(std::size_t partition_id, std::size_t coordinator_id) const override
	{
		DCHECK(coordinator_id < coordinator_num);
		return true;
	}

	bool is_backup() const override
	{
		return coordinator_id == 1;
	}
};

/*
 * There are 2 replicas in the system with N coordinators.
 * Coordinator 0 has a full replica.
 * The other replica is partitioned across coordinator 1 and coordinator N - 1
 *
 *
 * The master partition is partition id % N.
 *
 * case 1
 * If the master partition is from coordinator 1 to coordinator N - 1,
 * the secondary partition is on coordinator 0.
 *
 * case 2
 * If the master partition is on coordinator 0,
 * the secondary partition is from coordinator 1 to coordinator N - 1.
 *
 */

class StarSPartitioner : public Partitioner {
    public:
	StarSPartitioner(std::size_t coordinator_id, std::size_t coordinator_num)
		: Partitioner(coordinator_id, coordinator_num)
	{
		CHECK(coordinator_num >= 2);
	}

	~StarSPartitioner() override = default;

	std::size_t replica_num() const override
	{
		return 2;
	}

	bool is_replicated() const override
	{
		return true;
	}

	bool has_master_partition(std::size_t partition_id) const override
	{
		return master_coordinator(partition_id) == coordinator_id;
	}

	std::size_t master_coordinator(std::size_t partition_id) const override
	{
		return partition_id % coordinator_num;
	}

	bool is_partition_replicated_on(std::size_t partition_id, std::size_t coordinator_id) const override
	{
		DCHECK(coordinator_id < coordinator_num);

		auto master_id = master_coordinator(partition_id);
		auto secondary_id = 0u; // case 1
		if (master_id == 0) {
			secondary_id = partition_id % (coordinator_num - 1) + 1; // case 2
		}
		return coordinator_id == master_id || coordinator_id == secondary_id;
	}

	bool is_backup() const override
	{
		return false;
	}
};

class StarCPartitioner : public Partitioner {
    public:
	StarCPartitioner(std::size_t coordinator_id, std::size_t coordinator_num)
		: Partitioner(coordinator_id, coordinator_num)
	{
		CHECK(coordinator_num >= 2);
	}

	~StarCPartitioner() override = default;

	std::size_t replica_num() const override
	{
		return 2;
	}

	bool is_replicated() const override
	{
		return true;
	}

	bool has_master_partition(std::size_t partition_id) const override
	{
		return coordinator_id == 0;
	}

	std::size_t master_coordinator(std::size_t partition_id) const override
	{
		return 0;
	}

	bool is_partition_replicated_on(std::size_t partition_id, std::size_t coordinator_id) const override
	{
		DCHECK(coordinator_id < coordinator_num);

		if (coordinator_id == 0)
			return true;

		return coordinator_id == (partition_id % (coordinator_num - 1)) + 1;
	}

	bool is_backup() const override
	{
		return coordinator_id != 0;
	}
};

class CalvinPartitioner : public Partitioner {
    public:
	CalvinPartitioner(std::size_t coordinator_id, std::size_t coordinator_num, std::vector<std::size_t> replica_group_sizes)
		: Partitioner(coordinator_id, coordinator_num)
	{
		std::size_t size = 0;
		for (auto i = 0u; i < replica_group_sizes.size(); i++) {
			CHECK(replica_group_sizes[i] > 0);
			size += replica_group_sizes[i];

			if (coordinator_id < size) {
				coordinator_start_id = size - replica_group_sizes[i];
				replica_group_id = i;
				replica_group_size = replica_group_sizes[i];
				break;
			}
		}
		CHECK(std::accumulate(replica_group_sizes.begin(), replica_group_sizes.end(), 0u) == coordinator_num);
	}

	~CalvinPartitioner() override = default;

	std::size_t replica_num() const override
	{
		return replica_group_size;
	}

	bool is_replicated() const override
	{
		// replica group in calvin is independent
		return false;
	}

	bool has_master_partition(std::size_t partition_id) const override
	{
		return master_coordinator(partition_id) == coordinator_id;
	}

	std::size_t master_coordinator(std::size_t partition_id) const override
	{
		return partition_id % replica_group_size + coordinator_start_id;
	}

	bool is_partition_replicated_on(std::size_t partition_id, std::size_t coordinator_id) const override
	{
		// replica group in calvin is independent
		return false;
	}

	bool is_backup() const override
	{
		return false;
	}

    public:
	std::size_t replica_group_id;
	std::size_t replica_group_size;

    private:
	// the first coordinator in this replica group
	std::size_t coordinator_start_id;
};

class PartitionerFactory {
    public:
	static std::unique_ptr<Partitioner> create_partitioner(const std::string &part, std::size_t coordinator_id, std::size_t coordinator_num)
	{
		if (part == "hash") {
			return std::make_unique<HashPartitioner>(coordinator_id, coordinator_num);
		} else if (part == "hash2") {
			return std::make_unique<HashReplicatedPartitioner<2> >(coordinator_id, coordinator_num);
		} else if (part == "hash3") {
			return std::make_unique<HashReplicatedPartitioner<3> >(coordinator_id, coordinator_num);
		} else if (part == "hash4") {
			return std::make_unique<HashReplicatedPartitioner<4> >(coordinator_id, coordinator_num);
		} else if (part == "hash5") {
			return std::make_unique<HashReplicatedPartitioner<5> >(coordinator_id, coordinator_num);
		} else if (part == "hash6") {
			return std::make_unique<HashReplicatedPartitioner<6> >(coordinator_id, coordinator_num);
		} else if (part == "hash7") {
			return std::make_unique<HashReplicatedPartitioner<7> >(coordinator_id, coordinator_num);
		} else if (part == "hash8") {
			return std::make_unique<HashReplicatedPartitioner<8> >(coordinator_id, coordinator_num);
		} else if (part == "pb") {
			return std::make_unique<PrimaryBackupPartitioner>(coordinator_id, coordinator_num);
		} else if (part == "StarS") {
			return std::make_unique<StarSPartitioner>(coordinator_id, coordinator_num);
		} else if (part == "StarC") {
			return std::make_unique<StarCPartitioner>(coordinator_id, coordinator_num);
		} else if (part == "hpb") {
			return std::make_unique<HStorePBPartitioner>(coordinator_id, coordinator_num);
		} else if (part == "lotus") {
			return std::make_unique<HStorePBPartitioner>(coordinator_id, coordinator_num);
		} else {
			CHECK(false);
			return nullptr;
		}
	}
};

} // namespace star