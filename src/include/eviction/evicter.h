//#include "ltm/resource_tracker.h"
#include "storage/data_table.h"
#include "storage/temp_table.h"
namespace peloton {
namespace storage{
class DataTable;
}
namespace eviction {
class Evicter {
public:
    void EvictDataFromTable (storage::DataTable* table);
    storage::TempTable GetColdData(oid_t table_id, const std::vector<oid_t> &tiles_group_id, const std::vector<oid_t> &col_index_list);
private:
    void EvictTileGroup (std::shared_ptr<storage::TileGroup> *tg);
};

}
}
