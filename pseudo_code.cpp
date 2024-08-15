struct local_row {
        latch_t latch;
        bool is_valid;

        bool is_migrated;
        cxl_row *migrated_row;

        metadata_t meta;        // concurrency control metadata
        char data[];
}

struct cxl_row {
        latch_t latch;
        bool is_valid;

        metadata_t meta;        // concurrency control metadata
        char data[];
}

// index operations all take the bucket latch first, 
// do the operation, and then release the bucket latch.

bool read_write_access(uint64_t key, uint64_t host_id)
{
        bool ret = false;

        if (check_ownership(key, host_id)) {
                // case 1: local access
                local_row = search_local_index(key);
                local_row->latch();
                if (local_row->is_valid == true) {
                        // case 1.1 local row is not deleted
                        if (local_row->is_migrated == false) {
                                // case 1.1.1: row is not migrated
                                do_read_write(local_row);
                                ret = true;
                        } else {
                                // case 1.1.2: row is migrated to CXL
                                cxl_row = local_row->migrated_row;
                                cxl_row->latch();
                                if (cxl_row->is_valid == true) {
                                        // case 1.1.2.1: row is not deleted
                                        do_read_write(cxl_row);
                                        ret = true;
                                } else {
                                        // case 1.1.2.2: row is deleted
                                        // this case is impossible because delete is atomic 
                                        // and will remove the row from both local and cxl indexes
                                        assert(0);
                                }
                                cxl_row->unlatch();
                        }
                } else {
                        // case 1.2 local row is deleted
                        // do nothing
                        ret = false;
                }
                local_row->unlatch();
        } else {
                // case 2: shared region access
                cxl_row = search_cxl_index(key);           // take and release the bucket latch
                cxl_row->latch();
                if (cxl_row->is_valid == true) {
                        // case 2.1: row is not deleted
                        do_read_write(cxl_row);
                        ret = true;
                } else {
                        // case 2.2: row is deleted
                        // do nothing
                        ret = false;
                }
                cxl_row->unlatch();
        }

        return ret;
}

bool delete(uint64_t key, uint64_t host_id)
{
        bool ret = false;

        if (check_ownership(key, host_id)) {
                // case 1: local access
                local_row = search_local_index(key);
                local_row->latch();
                if (local_row->is_valid == true) {
                        // case 1.1 local row is not deleted
                        if (local_row->is_migrated == false) {
                                // case 1.1.1: local row is not migrated
                                remove_from_local_index(key, local_row);
                                local_row->is_valid = false;
                                register_ebr(local_row);  // register epoch-based memory reclaim
                                ret = true;
                        } else {
                                // case 1.1.2: row is migrated to CXL
                                cxl_row = local_row->migrated_row;
                                cxl_row->latch();
                                if (cxl_row->is_valid == true) {
                                        // case 1.1.2.1: cxl row is not deleted
                                        remove_from_cxl_index(key, cxl_row);
                                        remove_from_local_index(key, local_row);
                                        cxl_row->is_valid = false;
                                        local_row->is_valid = false;
                                        local_row->is_migrated = false;
                                        local_row->migrated_row = NULL;
                                        register_ebr(cxl_row);  // register epoch-based memory reclaim
                                        register_ebr(local_row);  // register epoch-based memory reclaim
                                        ret = true;
                                } else {
                                        // case 1.1.2.2: cxl row is deleted
                                        // this case is impossible because delete is atomic 
                                        // and will remove the row from both local and cxl indexes
                                        assert(0);
                                }
                                cxl_row->unlatch();
                        }
                } else {
                        // case 1.2 local row is deleted
                        // do nothing
                        ret = false;
                }
                local_row->unlatch();
        } else {
                // case 2: shared region access
                // this case is impossible because delete can only be performed by the owner
        }

        return ret;
}

bool data_move_in(uint64_t key, uint64_t host_id)
{
        bool ret = false;

        if (check_ownership(key, host_id)) {
                // case 1: I am the owner
                local_row = search_local_index(key);
                local_row->latch();
                if (local_row->is_valid == true) {
                        // case 1.1: the local row is not deleted
                        if (local_row->is_migrated == false) {
                                // case 1.1.1: row is not migrated
                                cxl_row = allocate_cxl_row();
                                cxl_row->latch();
                                copy_content(cxl_row, local_row);
                                insert_into_cxl_index(key, cxl_row);
                                local_row->is_migrated == true;
                                local_row->migrated_row = cxl_row;
                                cxl_row->is_valid = true;
                                cxl_row->unlatch();
                                ret = true;
                        } else {
                                // case 1.1.2: row is migrated to CXL
                                // do nothing since the row is already migrated
                                ret = false;
                        }
                } else {
                        // case 1.2: the local row is deleted
                        // do nothing
                        ret = false;
                }
                local_row->unlatch();
        } else {
                // case 2: I am not the owner
                // data can only be moved by its owner
                ret = false
        }

        return ret;
}

bool data_move_out(uint64_t key, uint64_t host_id)
{
        bool ret = false;

        if (check_ownership(key, host_id)) {
                // case 1: I am the owner
                local_row = search_local_index(key);
                local_row->latch();
                if (local_row->is_valid == true) {
                        // case 1.1: the local row is not deleted
                        if (local_row->is_migrated == false) {
                                // case 1.1.1: row is not migrated
                                // do nothing since the row is not migrated
                                ret = false;
                        } else {
                                // case 1.1.2: row is migrated to CXL
                                // do nothing since the row is already migrated
                                cxl_row = local_row->migrated_row;
                                cxl_row->latch();
                                copy_content(local_row, cxl_row);
                                remove_from_cxl_index(key, cxl_row);
                                local_row->is_migrated == false;
                                local_row->migrated_row = NULL;
                                cxl_row->is_valid = false;
                                cxl_row->unlatch();
                                ret = true;
                        }
                } else {
                        // case 1.2: the local row is deleted
                        // do nothing
                        ret = false;
                }
                local_row->unlatch();
        } else {
                // case 2: I am not the owner
                // data can only be moved by its owner
                ret = false
        }

        return ret;
}
