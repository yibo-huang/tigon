// Data Structures
struct phantom_meta_t {
        lock_t phantom_lock;
        cxl_row_t *next_tuple;
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
       bool ready_for_gc;       // for placeholders GC

       phantom_meta_t phantom_meta;     // phantom detection metadata
       metadata_t meta;         // concurrency control metadata
       char data[];
}

// Access Method
(key_existed, func_ret) lookup_and_apply(key, func, lookup_failure_handler) {
        if (check_ownership(key)) {
                        (key_existed, func_ret) = P.apply(key, local_tuple, lookup_failure_handler => {
                                if (local_tuple.is_migrated == false) {
                                        return func(local_tuple);
                                } else {
                                        return S.apply(local_tuple.cxl_tuple, cxl_tuple => {
                                                if (cxl_tuple.is_valid = true) {
                                                        return func(cxl_tuple);
                                                } else {
                                                        return false;
                                                }
                                        });
                                }
                        });
                        return (key_existed, func_ret);
        } else {
                // data might be moved out between data_move_in and S.apply
                // so we repeat this process until we succeed
                while (true) {
                        (key_existed, func_ret) = S.apply(key, cxl_tuple => {
                                if (cxl_tuple.is_valid = true) {
                                        return func(cxl_tuple);
                                } else {
                                        return false;
                                }
                        });
                        if (key_existed == false) {
                                if (data_move_in(key, lookup_failure_handler) == true) {
                                        continue;
                                } else {
                                        // if data_move_in() fails, then the remote host would call the lookup_failure_handler,
                                        // so we do not need to do anything here
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
                return local_insert_handler(key, tuple);
        } else {
                if (remote_insert_prepare(key) == true) {
                        return S.apply(key, cxl_tuple => {
                                next_cxl_tuple = cxl_tuple.next_key_tuple;
                                (key_existed, lock_success) = S.apply(next_cxl_tuple, cxl_tuple => {
                                        return cxl_tuple.phantom_lock.take_lock(WRITE);
                                });
                                if (lock_success == true) {
                                        cxl_tuple.phantom_lock.take_lock(WRITE);
                                        cxl_tuple.copy(tuple);
                                        cxl_tuple.is_valid = true;      // OCC will have an absent bit in metadata
                                        return true;
                                } else {
                                        return false;
                                }
                        })
                } else {
                        return false;
                }
        }
}

bool update_sibling_information(key, current_tuple) {
        // 1. take the latch of the left, right siblings and myself
        left_sibling = P.get_left_sibling(key);
        if (left_sibling == nullptr)
                return false;

        right_sibling = P.get_right_sibling(key);

        (key_existed, func_ret) = P.apply(left_sibling, tuple => {
                if (tuple.is_migrated == false) {
                        return true;
                } else {
                        return S.apply(tuple.cxl_tuple, cxl_tuple => {
                                if (cxl_tuple.is_valid = true) {
                                        cxl_tuple.phantom_meta.right_sibling = new_right_sibling;
                                        return true;
                                } else {
                                        return false;
                                }
                        });
                }
        });
        // 2. release the latch of the left, right siblings and myself
}

bool local_insert_handler(key, tuple) {
        // B+-Tree atomic section start

        // take the phantom lock of the next key
        next_local_tuple = P.get_next_tuple(key);
        (key_existed, lock_success) = P.apply(next_local_tuple, local_tuple => {
                local_tuple.meta.phantom_lock.take_lock(WRITE);
        });

        if (lock_success == true) {
                // take the phantom lock and insert the tuple
                tuple.meta.phantom_lock.take_lock(WRITE);
                insert_success = P.insert(key, tuple);
                if (insert_success == true) {
                        // update next_tuple information
                        update_sibling_information(key, nullptr);
                        return true;
                } else {
                        (key_existed, lock_success) = P.apply(next_local_tuple, local_tuple => {
                                local_tuple.meta.phantom_lock.release_lock(WRITE);
                        });
                        assert(key_existed == true && func_ret == true);
                        return false;
                }
        } else {
                (key_existed, lock_success) = P.apply(next_local_tuple, local_tuple => {
                        local_tuple.meta.phantom_lock.release_lock(WRITE);
                });
                return false;
        }

        // B+-Tree atomic section ends
}

// Note that this function creates a situation
// where the local tuple is migrated but the cxl tuple is invalid.
// Our lookup_and_apply function handles this case
bool remote_insert_prepare(key) {
        // B+-Tree atomic section start

        // create tuples
        cxl_tuple = create_cxl_tuple(
                this->is_valid = false;
        );
        local_tuple = create_local_tuple(
                this->is_migrated = true;
                this->cxl_tuple = cxl_tuple;
        );

        // atomically insert a placeholder and migrate the next-key
        success = P.insert(key, local_tuple);
        if (success == true) {
                ret = S.insert(key, cxl_tuple);               // guaranteed to succeed
                // update next_tuple information
                update_sibling_information(key, nullptr);
                assert(ret == true);
                data_move_in(P.get_next_tuple(key));      // data_move_in will call update_sibling_information()
        } else {
                free(local_tuple);
                free(cxl_tuple);
        }

        // B+-Tree atomic section ends
        return success;
}

bool remote_insert_handler(key, tuple) {
        // B+-Tree atomic section start

        // take the phantom lock of the next key
        next_cxl_tuple = S.get_next_tuple(key);
        (key_existed, lock_success) = S.apply(next_cxl_tuple, cxl_tuple => {
                cxl_tuple.meta.phantom_lock.take_lock(WRITE);
        });

        if (lock_success == true) {
                // take the phantom lock of the placeholder and set it as valid
                (key_existed, func_ret) = S.apply(key, cxl_tuple => {
                        cxl_tuple.meta.phantom_lock.take_lock(WRITE);
                        cxl_tuple.copy(tuple);
                        cxl_tuple.is_valid = true;
                });
                assert(key_existed == true && func_ret == true);
                return true;
        } else {
                // GC placeholder
                (key_existed, func_ret) = S.apply(key, cxl_tuple => {
                        cxl_tuple.ready_for_gc = true;
                });
                assert(key_existed == true && func_ret == true);
                // release phantom lock
                (key_existed, func_ret) = P.apply(next_local_tuple, local_tuple => {
                        local_tuple.meta.phantom_lock.release_lock(WRITE);
                });
                assert(key_existed == true && func_ret == true);
                return false;
        }

        // B+-Tree atomic section ends
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
                                if (data_move_in(key, lookup_failure_handler) == true) {
                                        continue;
                                } else {
                                        // if data_move_in() fails, then the remote host should migrate the index node
                                        // the lookup_failure_handler will add it to the access set   
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

void scan(min_key, max_key, scan_callback_func, func) {
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

// scan_callback_func will migrate the index node
Buffer remote_scan_handler(min_key, max_key) {
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
                S.insert(key, cxl_tuple);
                update_sibling_information(key, cxl_tuple);
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

