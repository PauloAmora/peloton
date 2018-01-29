#include "eviction/evicter.h"
#include "type/serializeio.h"
#include "type/ephemeral_pool.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "storage/tuple_iterator.h"
#include "storage/temp_table.h"
#include "util/file_util.h"
#include "storage/tile.h"
#include <string>

#include "util/output_buffer.h"
#include "gc/gc_manager_factory.h"

#include "storage/storage_manager.h"

namespace peloton  {
namespace eviction {
//storage::TempTable GetColdData(oid_t table_id, oid_t tg_id);
    //decidir qual e decidir quando cria-lo??
    const std::string DIR_GLOBAL = { "/home/paulo/log/" };

    void Evicter::EvictDataFromTable(storage::DataTable* table) {
        //auto gc = gc::GCManagerFactory::GetInstance();

        for (uint offset = 0; offset < table->GetTileGroupCount(); offset++) {
            auto tg = table->GetTileGroup(offset);

            if (tg->GetHeader()->IsEvictable()) {
                if (!FileUtil::CheckDirectoryExistence(
                            (DIR_GLOBAL + std::to_string(tg->GetTableId())).c_str()))
                    if (!FileUtil::CreateDirectory(
                                (DIR_GLOBAL + std::to_string(tg->GetTableId())).c_str(), 0700))
                        LOG_DEBUG("ERROR - CREATE DIRECTORY");
                        //throw exception;

                EvictTileGroup(&tg);
                table->DeleteTileGroup(offset);
                tg.reset();
            }

        }
        table->CompactTgList();

    //auto temp_table = GetColdData(33554540, 38);

//    std::cout << "TUPLE_COUNT_IN_TEMP: " << temp_table.GetTupleCount() << std::endl;

//    oid_t found_tile_group_count = temp_table.GetTileGroupCount();

//    for (oid_t tile_group_itr = 0; tile_group_itr < found_tile_group_count;
//         tile_group_itr++) {
//      auto tile_group = temp_table.GetTileGroup(tile_group_itr);
//      auto tile_count = tile_group->GetTileCount();

//      for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
//        storage::Tile *tile = tile_group->GetTile(tile_itr);
//        if (tile == nullptr) continue;
//        storage::Tuple tuple(tile->GetSchema());
//        storage::TupleIterator tuple_itr(tile);
//        while (tuple_itr.Next(tuple)) {
//          auto tupleVal = tuple.GetValue(0);
////          EXPECT_FALSE(tupleVal.IsNull());
//          //tuple.GetInfo()
//            std::cout << tupleVal << std::endl;
//        }

//      }

    //}

    }

void SerializeMap(std::shared_ptr<storage::TileGroup> *tg) {
    CopySerializeOutput output;
    FileHandle f;
    OutputBuffer *bf = new OutputBuffer();

    FileUtil::OpenFile((DIR_GLOBAL + std::to_string((*tg)->GetTableId()) + "/" +
                        std::to_string((*tg)->GetTileGroupId()) + "_h").c_str(), "wb", f);

    for (const auto &mapping : (*tg)->GetColumnMap()) {
        output.WriteInt(mapping.first);
        output.WriteInt(mapping.second.first);
        output.WriteInt(mapping.second.second);
    }

    bf->WriteData(output.Data(), output.Size());

    fwrite((const void *) (bf->GetData()), bf->GetSize(), 1, f.file);

    //  Call fsync
    FileUtil::FFlushFsync(f);
    FileUtil::CloseFile(f);
    delete bf;
}

column_map_type DeserializeMap(oid_t table_id, oid_t tg_id) {
    FileHandle f;
    FileUtil::OpenFile((DIR_GLOBAL + std::to_string(table_id) + "/" +
                        std::to_string(tg_id) + "_h").c_str(), "rb", f);

    auto table = storage::StorageManager::GetInstance()->GetTableWithOid(
        16777316, table_id);
    auto schema = table->GetSchema();
    auto column_count = schema->GetColumnCount();
    size_t buf_size = column_count * 3 * 4;
    std::unique_ptr<char[]> buffer(new char[buf_size]);

    FileUtil::ReadNBytesFromFile(f,  buffer.get(), buf_size);

    CopySerializeInput input_decode((const void *) buffer.get(), buf_size);

    column_map_type map_recovered;

    for (auto i = 0U; i < column_count; ++i) {
        oid_t col_id = (oid_t) input_decode.ReadInt();
        oid_t til_id = (oid_t) input_decode.ReadInt();
        oid_t offset = (oid_t) input_decode.ReadInt();

        map_recovered[col_id] = std::make_pair(til_id, offset);
    }

    FileUtil::CloseFile(f);

    return map_recovered;

}

//storage::TempTable GetColdData(oid_t table_id, const std::vector<oid_t> &tiles_group_id, const std::vector<oid_t> &col_index_list) {
//    auto table = storage::StorageManager::GetInstance()->GetTableWithOid(
//        16777316, table_id);
//    auto schema = table->GetSchema();
//    auto temp_schema = catalog::Schema::CopySchema(schema, index_list);
//    //ver qual oid                                              //, table->GetLayoutType()
//    storage::TempTable temp_table(INVALID_OID, temp_schema, true);

//    for (auto tg_id : tiles_group_id) {
//        auto column_map = DeserializeMap(table_id, tg_id);
//        auto pair = column_map[col_index];
//        auto tile_id = pair.first;
//        auto offset  = pair.second;
//    }




//    size_t buf_size = temp_schema->GetColumnCount() * 4 * 5; //ver o descarte
//    std::unique_ptr<char[]> buffer(new char[buf_size]);

//    FileHandle f;
//    FileUtil::OpenFile((DIR_GLOBAL + std::to_string(table_id) + "/" +
//                        std::to_string(tg_id) + "_" +
//                        std::to_string(tile_id)).c_str(), "rb", f);

//    FileUtil::ReadNBytesFromFile(f,  buffer.get(), buf_size);

//    CopySerializeInput record_decode((const void *)buffer.get(), buf_size);

//    for (oid_t tuple_count = 0; tuple_count < 5; tuple_count++) {
//        std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(temp_schema, true));

//        for (oid_t oid = 0; oid < temp_schema->GetColumnCount(); oid++) {
//            type::Value val = type::Value::DeserializeFrom(
//                        record_decode, temp_schema->GetColumn(oid).GetType());
////            std::cout << val << std::endl;
//            tuple->SetValue(oid, val);
//        }

//        temp_table.InsertTuple(tuple.get());
//    }

//    FileUtil::CloseFile(f);

//    return temp_table;
//}

    void Evicter::EvictTileGroup(std::shared_ptr<storage::TileGroup> *tg) {
        CopySerializeOutput output;
        FileHandle f;
        OutputBuffer *bf = new OutputBuffer();

        SerializeMap(tg);

        for (uint offset = 0; offset < (*tg)->GetTileCount(); offset++) {
            auto tile = (*tg)->GetTile(offset);
            for (uint i = 0; i < (*tg)->GetActiveTupleCount(); i++){
                type::Value val = tile->GetValue(i,0);
                val.SerializeTo(output);
            }

//            tile->SerializeTo(output, (*tg)->GetActiveTupleCount());


//            if (!FileUtil::CreateFile((DIR_GLOBAL + std::to_string((*tg)->GetTableId()) + "/" +
//                                       std::to_string((*tg)->GetTileGroupId())).c_str()))
//                LOG_DEBUG("ERROR - CREATE FILE");

            FileUtil::OpenFile((DIR_GLOBAL + std::to_string((*tg)->GetTableId()) + "/" +
                                std::to_string((*tg)->GetTileGroupId()) + "_" + std::to_string(offset)).c_str(), "wb", f);
            bf->WriteData(output.Data(), output.Size());

            fwrite((const void *) (bf->GetData()), bf->GetSize(), 1, f.file);

            //  Call fsync
                FileUtil::FFlushFsync(f);
                FileUtil::CloseFile(f);
                delete bf;
        }
    }

}
}
