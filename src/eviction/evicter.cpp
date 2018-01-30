#include "eviction/evicter.h"
#include "type/serializeio.h"
#include "type/value_factory.h"
#include "type/ephemeral_pool.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "storage/tuple_iterator.h"
#include "storage/temp_table.h"
#include "util/file_util.h"
#include "storage/tile.h"
#include <string>

#include <utility>

#include "util/output_buffer.h"
#include "gc/gc_manager_factory.h"

#include "storage/storage_manager.h"

namespace peloton  {
namespace eviction {
storage::TempTable GetColdData(oid_t table_id, const std::vector<oid_t> &tiles_group_id, const std::vector<oid_t> &col_index_list);
    //decidir qual e decidir quando cria-lo??
    const std::string DIR_GLOBAL = { "/home/paulo/log/" };

    void Evicter::EvictDataFromTable(storage::DataTable* table) {
        //auto gc = gc::GCManagerFactory::GetInstance();

        for (uint offset = 0; offset < table->GetTileGroupCount(); offset++) {
            auto tg = table->GetTileGroup(offset);

            if (tg->GetHeader()->IsEvictable()) {
                if (!FileUtil::CheckDirectoryExistence(
                            (DIR_GLOBAL + std::to_string(tg->GetTableId())).c_str())){
                    if (!FileUtil::CreateDirectory(
                                (DIR_GLOBAL + std::to_string(tg->GetTableId())).c_str(), 0700)){
                        LOG_DEBUG("ERROR - CREATE DIRECTORY");
                        //throw exception;
                    }
}
                EvictTileGroup(&tg);
                table->DeleteTileGroup(offset);
                tg.reset();
            }

        }

        std::vector<oid_t> tiles_group_id;
        tiles_group_id.push_back(43);

        std::vector<oid_t> col_index_list;
        col_index_list.push_back(1);
//        col_index_list.push_back(1);
//        col_index_list.push_back(2);
        col_index_list.push_back(2);
        col_index_list.push_back(5);

    auto temp_table = GetColdData(33554540, tiles_group_id, col_index_list);

//    std::cout << "TUPLE_COUNT_IN_TEMP: " << temp_table.GetTupleCount() << std::endl;

    oid_t found_tile_group_count = temp_table.GetTileGroupCount();

    for (oid_t tile_group_itr = 0; tile_group_itr < found_tile_group_count;
         tile_group_itr++) {
      auto tile_group = temp_table.GetTileGroup(tile_group_itr);
      auto tile_count = tile_group->GetTileCount();

      for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
        storage::Tile *tile = tile_group->GetTile(tile_itr);
        if (tile == nullptr) continue;
        storage::Tuple tuple(tile->GetSchema());
        storage::TupleIterator tuple_itr(tile);
        while (tuple_itr.Next(tuple)) {
            for (auto i = 0U; i < col_index_list.size(); i++) {
                auto tupleVal = tuple.GetValue(i);
                std::cout << tupleVal << std::endl;
            }
        }

      }

    }

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
//col_index_list have to be in ascending order
storage::TempTable GetColdData(oid_t table_id, const std::vector<oid_t> &tiles_group_id, const std::vector<oid_t> &col_index_list) {
    auto table = storage::StorageManager::GetInstance()->GetTableWithOid(
        16777316, table_id);
    auto schema = table->GetSchema();
    auto temp_schema = catalog::Schema::CopySchema(schema, col_index_list);
    //ver qual oid                                              //, table->GetLayoutType()
    storage::TempTable temp_table(INVALID_OID, temp_schema, true);

    char num_col_buf[4]; //sizeof(int32_t)

    size_t buf_size = 4096;
    std::unique_ptr<char[]> buffer(new char[buf_size]);

    for (auto tg_id : tiles_group_id) {
        auto column_map = DeserializeMap(table_id, tg_id);
        std::vector<std::unique_ptr<storage::Tuple>> recovered_tuples;
        //      tile_id idx_col_in_temp, offset
        std::map<oid_t, std::vector<std::pair<oid_t, oid_t>>> tiles_to_recover;

        //  Pegar quais tiles ler
        for (oid_t i = 0; i < col_index_list.size(); i++) {
            auto col_idx = col_index_list[i];

            auto pair_it = column_map.find(col_idx);

            // eh necessario essa verificacao?? acho q nao
            if (pair_it == column_map.end())
                continue;

            auto tile_id = pair_it->second.first;
            auto offset  = pair_it->second.second;

            if (tiles_to_recover.find(tile_id) == tiles_to_recover.end()) {
                std::vector<std::pair<oid_t, oid_t>> entries;

                tiles_to_recover[tile_id] = entries;
            }

            tiles_to_recover[tile_id].push_back(std::make_pair(i, offset));

        }

        //Ler tiles e construir tuplas

        for (oid_t tuple_count = 0; tuple_count < 5; tuple_count++) {
            std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(temp_schema, true));

            recovered_tuples.push_back(std::move(tuple));
        }

        for (auto tile_it : tiles_to_recover) {
            auto tile_id = tile_it.first;
            auto cols_offsets = tile_it.second;
            FileHandle f;

            FileUtil::OpenFile((DIR_GLOBAL + std::to_string(table_id) + "/" +
                                std::to_string(tg_id) + "_" +
                                std::to_string(tile_id)).c_str(), "rb", f);

            FileUtil::ReadNBytesFromFile(f, num_col_buf, 4);
            CopySerializeInput num_col_decode((const void *) &num_col_buf, 4);

            oid_t num_col = num_col_decode.ReadInt();

            for (oid_t tuple_count = 0; tuple_count < 5; tuple_count++) {
                FileUtil::ReadNBytesFromFile(f, buffer.get(), num_col * 4);
                CopySerializeInput tuple_decode((const void *) buffer.get(), num_col * 4);

                oid_t offset_current = 0;

                for (oid_t i = 0; i < cols_offsets.size(); i++) {
                    auto col_oid = cols_offsets[i].first;
                    auto offset = cols_offsets[i].second;

                    //pulando
                    while (offset_current < offset) {
                        tuple_decode.ReadInt();
                        offset_current++;
                    }

                    if (offset_current == offset) {
                        type::Value val = type::Value::DeserializeFrom(
                                    tuple_decode, temp_schema->GetColumn(col_oid).GetType());
                        offset_current++;
                        LOG_DEBUG("VALUE RETRIEVED: %d", val.GetAs<int>());
                        recovered_tuples[tuple_count]->SetValue(col_oid, val);
                    } else {
                        std::cout << "ERRORRRRRR!!!!!!! offset_current > offset";
                    }

                }

            }

            FileUtil::CloseFile(f);

        }

        for (auto &tuple_ptr : recovered_tuples)
            temp_table.InsertTuple(tuple_ptr.get());

    }

    return temp_table;
}

    void Evicter::EvictTileGroup(std::shared_ptr<storage::TileGroup> *tg) {
        CopySerializeOutput output;
        FileHandle f;
        OutputBuffer *bf = new OutputBuffer();

        SerializeMap(tg);

        for (uint offset = 0; offset < (*tg)->GetTileCount(); offset++) {
            auto tile = (*tg)->GetTile(offset);
            auto tile_col_count = tile->GetColumnCount();

            output.WriteInt(tile_col_count);
            //type::ValueFactory::GetIntegerValue(tile_col_count).SerializeTo(output);
            for (oid_t tuple_offset = 0; tuple_offset < (*tg)->GetActiveTupleCount();
                 tuple_offset++) {
                 for (oid_t col_id = 0; col_id < tile_col_count; col_id++) {
                    type::Value val = tile->GetValue(tuple_offset, col_id);
                    val.SerializeTo(output);

                }
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
