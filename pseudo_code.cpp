// Data Structure

struct local_index_node_t {
        uint64_t version_num;

        bool is_migrated;
        cxl_index_node_t *migrated_node;

        key_value_pairs[];

        // other fields
}

struct cxl_index_node_t {
        uint64_t version_num;
}

struct local_row_t {
        latch_t latch;
        bool is_valid;

        bool is_migrated;
        cxl_row *migrated_row;

        metadata_t meta;        // concurrency control metadata
        char data[];
}

struct cxl_row_t {
        latch_t latch;
        bool is_valid;
        bool ready_for_gc;

        metadata_t meta;        // concurrency control metadata
        char data[];
}

// Access Method

void transaction::silo_lookup_failure_handler(index_node)
{
        this->index_node_access_set.add(index_node);
}

(key_existed, func_ret) lookup_and_apply(key, lookup_failure_handler, func) {
	if (check_ownership(key)) {
                (key_existed, func_ret) = P.apply(key, lookup_failure_handler, local_tuple => {
			if (local_tuple.is_migrated == false) {
                                return func(local_tuple);
                        } else {
                                return S.apply(local_tuple.cxl_tuple, cxl_tuple => {
                                        if (cxl_tuple.is_valid = true) {
                                                func(cxl_tuple);
                                                return true;
                                        } else {
                                                return false;
                                        }
                                });
                        }
                })
                return (key_existed, func_ret);
        } else {
                // data might be moved out between data_move_in and S.apply
                // so we repeat this process until we succeed
                while (true) {
                        (key_existed, func_ret) = S.apply(key, cxl_tuple => {
                                if (cxl_tuple.is_valid = true)
                                        return func(cxl_tuple);
                                else
                                        return false;
                        });
                        if (key_existed == false) {
                                // optimization opportunity (optional)
                                cxl_index_node_t cxl_index_node = S.get_cxl_index_node(key);
                                if (cxl_index_node != nullptr)
                                        if (cxl_index_node->find_key(key) == false)
                                                return (false, false);

                                if (data_move_in(key) == true) {
                                        continue;
                                } else {
                                        // if data_move_in() fails, then the remote host should migrate the index node
                                        lookup_failure_handler(S.get_migrated_index_node(key));
                                        return (false, false);
                                }
                        } else {
                                return (key_existed, func_ret);
                        }
                }
        }
}

// for 2PL, 'tuple' is the the regular tuple to be inserted
// for OCC, 'tuple' should be a placeholder, whose absent bit is already set to 1
bool insert(key, tuple) {
        if (check_ownership(key)) {
                return P.insert(key, tuple);    // will insert to the migrated index node if the index node is migrated
        } else {
                if (remote_insert_prepare(key) == true)
                        return S.apply(key, cxl_tuple => {
                                cxl_tuple.copy(tuple);
                                cxl_tuple.is_valid = true;      // OCC will have an absent bit in metadata
                        })
                else
                        return false;
        }
}

// Note that this function creates a situation
// where the local tuple is migrated but the cxl tuple is invalid.
// Our lookup_and_apply function handles this case
bool remote_insert_prepare(key) {
        // Note: need GC if we fail to insert
        cxl_tuple = create_cxl_tuple(
                this->is_valid = false;
        );
        local_tuple = create_local_tuple(
                this->is_migrated = true;
                this->cxl_tuple = cxl_tuple;
        );

        // we may not need these latches,
        // but adding them simplifies reasoning.
        local_tuple.latch();
        success = P.insert(key, local_tuple)
        if (success == true)
                S.insert(key, cxl_tuple);       // guaranteed to succeed
        local_tuple.unlatch();

        return success;
}

// executed by the remote host
// mark the placeholder as GC-able
bool remote_insert_abort(key) {
        S.apply(key, tuple => {
                tuple.ready_for_gc = true;
        });
}

bool delete(key, lookup_failure_handler) {
        if (check_ownership(key)) {
                return P.delete(key, local_tuple => {
                        if (local_tuple.is_migrated == false)
                                return true;
                        else
                                return S.delete(key, cxl_tuple => {
                                        if (cxl_tuple.is_valid == true)
                                                return true;
                                        else
                                                return false;
                                })
                })
        } else {
                // data might be moved out between data_move_in and S.apply
                // so we repeat this process until we succeed
                while (true) {
                        // delete the tuple by marking it as invalid
			(key_existed, func_ret) = S.apply(key, cxl_tuple => {
				if (cxl_tuple.is_valid == true) {
                                        cxl_tuple.is_valid = false;
                                        return true;
                                } else {
                                        return false;
                                }
                        });
                        if (key_existed == false) {
                                if (data_move_in(key) == true) {
                                        continue;
                                } else {
                                        // if data_move_in() fails, then the remote host should migrate the index node
                                        lookup_failure_handler(S.get_migrated_index_node(key));
                                        return (false, false);
                                }
                        } else {
                                return true;
                        }
                }
        }
}

// executed by the remote host
// mark the placeholder as GC-able
bool remote_delete_cleanup(key) {
        S.apply(key, tuple => {
                tuple.ready_for_gc = true;
        });
}

void range_scan(min_key, max_key, scan_callback_func, func) {
        if (check_ownership(min_key, max_key)) {
                (key_existed, func_ret) = P.scan(key, scan_callback_func, local_tuple => {
			if (local_tuple.is_migrated == false) {
                                return func(local_tuple);
                        } else {
                                return S.apply(local_tuple.cxl_tuple, cxl_tuple => {
                                        if (cxl_tuple.is_valid = true) {
                                                func(cxl_tuple);
                                                return true;
                                        } else {
                                                return false;
                                        }
                                });
                        }
                });
        } else {
                scan_results = remote_scan_handler(min_key, max_key);
                for (row : scan_results)
                        func(row);
                for (index_node_version_number : scan_results)
                        scan_callback_func(index_node_version_number);
        }
}

void transaction::silo_scan_callback(index_node)
{
        if (local)
                index_node_access_set.add(index_node);
        else {
                buffer.write(index_node.version_number);
                cxl_index_node = migrate_index_node(index_node);
                buffer.write(&cxl_index_node);
        }
}

// scan_callback_func will migrate the index node
Buffer remote_scan_handler(min_key, max_key, scan_callback_func) {
        buffer = P.scan(key, scan_callback_func, local_tuple => {
                Buffer buffer[];
                if (local_tuple.is_migrated == false) {
                        buffer.write(local_tuple);
                } else {
                        return S.apply(local_tuple.cxl_tuple, cxl_tuple => {
                                if (cxl_tuple.is_valid = true) {
                                        buffer.write(cxl_tuple);
                                } else {
                                        return false;
                                }
                        });
                }
                return buffer;
        });
        return buffer;
}

bool data_move_in(key) {
        (key_existed, func_ret) = P.apply(key, local_tuple => {
                if (has_enough_cxl_memory(local_tuple) == false)
                        data_move_out(size(local_tuple));
                cxl_tuple = create_cxl_tuple(
                        this->is_valid = true;
                );
                local_tuple.cxl_tuple = cxl_tuple;
                local_tuple.is_migrated = true;
                return S.insert(key, cxl_tuple);
        });
        return func_ret;
}

void data_move_out(size) {
        deallocated_size = 0;
        while (true) {
                victim_keys = get_victims(P, size);
                for (key : victim_keys) {
                        P.apply(key, local_tuple => {
                                if (local_tuple.is_migrated == true) {
                                        S.apply(local_tuple.cxl_tuple, cxl_tuple => {
                                                cxl_tuple.is_valid = false;
                                                S.delete(key, true);
                                        });
                                        local_tuple.copy(cxl_tuple);
                                        local_tuple.is_migrated = false;
                                        deallocated_size += size(cxl_tuple);
					deallocate(cxl_tuple);
                                }
                        });
                }
                if (deallocated_size >= size)
                        break;
        }
}


// Concurrency Control: 2PL NO_WAIT
two_pl_read(key, data) {
        (key_existed, func_ret) = lookup_and_apply(key, tuple => {
                if (check_lock_bit_conflicts(tuple, READ_LOCK) == true) {
                        return ABORT;   \\ NO_WAIT
                } else {
                        tuple.metadata.read_lock_bit = 1;
                        memcpy(data, tuple.data, size(tuple.data));
                        return true;
                }
        })
        if (key_existed == false)
                // TODO: phantom detection
        return (key_existed, func_ret);
}

two_pl_write(key, data) {
        (key_existed, func_ret) = lookup_and_apply(key, tuple => {
                if (check_lock_bit_conflicts(tuple, WRITE_LOCK) == true) {
                        return ABORT;   \\ NO_WAIT
                } else {
                        tuple.metadata.write_lock_bit = 1;
                        memcpy(tuple.data, data, size(tuple.data));
                        return true;
                }
        })
        if (key_existed == false)
                // TODO: phantom detection
        return (key_existed, func_ret);
}

two_pl_insert(key, tuple) {
        tuple.metadata.read_lock_bit = 1;
        tuple.metadata.write_lock_bit = 1;
        if (insert(key, tuple) == false)
                return ABORT;
        return true;
}

// used before commit
two_pl_delete_step_0(key) {
        (key_existed, func_ret) = lookup_and_apply(key, tuple => {
                if (check_lock_bit_conflicts(tuple, READ_WRITE_LOCK) == true) {
                        return ABORT;   \\ NO_WAIT
                } else {
                        tuple.metadata.read_lock_bit = 1;
                        tuple.metadata.write_lock_bit = 1;  // lock bit prevent others from deleting the tuple
                }
        })
}

// used after we know we can commit
two_pl_delete_step_1(key) {
        delete(key);
}

two_pl_lock_release(key) {
        (key_existed, func_ret) = lookup_and_apply(key, tuple => {
                tuple.metadata.read_lock_bit = 0;
                tuple.metadata.write_lock_bit = 0;
                return true;
        })
}

// Concurrency Control: Silo
// used in the execution phase
silo_read(key, metadata, data) {
        (key_existed, func_ret) = lookup_and_apply(key, tuple => {
                metadata = tuple.metadata;
                memcpy(data, tuple.data, size(tuple.data));
                return true;
        })
        update_readset();
        return (key_existed, func_ret);
}

silo_insert_execution_phase(key) {
        placeholder = create_placeholder();
        update_readset();
        update_writeset();
        return insert(key, placeholder);
}

silo_delete_execution_phase(key, metadata) {
        (key_existed, func_ret) = lookup_and_apply(key, tuple => {
                metadata = tuple.metadata;
                return true;
        })
        update_readset();
        update_writeset();
        return (key_existed, func_ret);
}

// used in the validation phase
lock_write_set(write_set) {
        for (local_tuple : write_set) {
                while (true) {
                        (key_existed, func_ret) = lookup_and_apply(local_tuple, tuple => {
                                if (check_lock_bit_conflicts(tuple, WRITE_LOCK) == true)
                                        return false;
                                tuple.metadata.write_lock_bit = 1;
                                return true;
                        })
                        if (func_ret == true)
                                break;
                }
        }
}

validate_read_set(read_set) {
        for (local_tuple : write_sets) {
                (key_existed, metadata) = lookup_and_apply(local_tuple, tuple => {
                        return tuple.metadata;
                })
                validate(metadata);
        }
}

// used in the commit phase
apply_writes(write_set) {
        for (local_tuple : write_set) {
                while (true) {
                        (key_existed, func_ret) = lookup_and_apply(local_tuple, tuple => {
                                memcpy(tuple.data, local_tuple.)
                                return true;
                        })
                        if (func_ret == true)
                                break;
                }
        }
}

apply_inserts(insert_set) {
        for (local_tuple : insert_set) {
                (key_existed, func_ret) = lookup_and_apply(local_tuple, tuple => {
                        tuple.metadata.is_absent = false;
                })
        }
}

apply_deletes(delete_set) {
        for (local_tuple : delete_set) {
                (key_existed, func_ret) = lookup_and_apply(local_tuple, tuple => {
                        tuple.metadata.is_absent = true;
                })
                register_garbage_collection(local_tuple);
        }
}
