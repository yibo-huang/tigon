#include <atomic>

#include "protocol/Pasha/MigrationManager.h"

namespace star {

MigrationManager *migration_manager;

std::atomic<uint64_t> num_data_move_in{ 0 };
std::atomic<uint64_t> num_data_move_out{ 0 };

}
