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
#include "storage/zone_map_manager.h"
#include "concurrency/transaction_manager_factory.h"
#include "concurrency/transaction_manager.h"
#include <string>

#include <utility>

#include "util/output_buffer.h"
#include "gc/gc_manager_factory.h"

#include "catalog/catalog.h"

namespace peloton  {
namespace eviction {

column_map_type types[] = {
    { {0, {0,0}},  {1, {1,0}}, {2, {2,0}}, {3, {2,1}}, {4, {2,2}}, {5, {2,3}}, {6, {2,4}}, {7, {2,5}}, {8, {2,6}}, {9, {2,7}}, {10, {2,8}} },
    { {0, {0,0}},  {1, {1,0}}, {2, {1,1}}, {3, {2,0}}, {4, {2,1}}, {5, {2,2}}, {6, {3,0}}, {7, {3,1}}, {8, {3,2}}, {9, {3,3}}, {10, {3,4}} },
    { {0, {0,0}},  {1, {1,0}}, {2, {2,0}}, {3, {2,1}}, {4, {2,2}}, {5, {3,0}}, {6, {3,1}}, {7, {3,2}}, {8, {3,3}}, {9, {3,4}}, {10, {3,5}} },
    { {0, {0,0}},  {1, {1,0}}, {2, {2,0}}, {3, {2,1}}, {4, {3,0}}, {5, {3,1}}, {6, {3,2}}, {7, {4,0}}, {8, {4,1}}, {9, {5,0}}, {10, {5,1}} },
    { {0, {0,0}},  {1, {1,0}}, {2, {1,1}}, {3, {2,0}}, {4, {2,1}}, {5, {2,2}}, {6, {3,0}}, {7, {4,0}}, {8, {4,1}}, {9, {5,0}}, {10, {5,1}} }
};

storage::TempTable GetColdData(oid_t table_id, const std::vector<oid_t> &tiles_group_id, const std::vector<oid_t> &col_index_list);
    //decidir qual e decidir quando cria-lo??
    const std::string DIR_GLOBAL = { "/home/paulo/log/" };

    void Evicter::EvictDataFromTable(storage::DataTable* table) {
        auto zone_map_manager = storage::ZoneMapManager::GetInstance();
        auto schema = table->GetSchema();
        if(table->GetFilterMap().count(0) == 0){
            for (uint col_itr = 0; col_itr < schema->GetColumnCount(); col_itr++){
                if(schema->GetColumn(col_itr).GetType() != type::Type::TypeId::VARCHAR)
                {
                    cuckoofilter::CuckooFilter<int32_t, 12>* f = new cuckoofilter::CuckooFilter<int32_t, 12>(500000);
                    table->GetFilterMap().insert(std::make_pair(col_itr, f));
                }
            }
        }
        /*for (uint offset = 0; offset < table->GetTileGroupCount(); offset++) {
            auto tg = table->GetTileGroup(offset);
        //    LOG_DEBUG("--- Tuples in tilegroup %u: %u", tg->GetTileGroupId(), tg->GetActiveTupleCount());

        }*/
        for (uint offset = 0; offset < table->GetTileGroupCount(); offset++) {
           // table->TransformTileGroup(offset, types[offset%5]);
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
                oid_t column_count = schema->GetColumnCount();
                for(uint i = 0; i < tg->GetActiveTupleCount(); i++){
                for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
                    if(schema->GetColumn(column_itr).IsInlined()){
                        table->GetFilterMap().at(column_itr)->Add(tg->GetValue(i,column_itr).GetAs<int32_t>());
                    }
                }
                }
                EvictTileGroup(&tg);
                zone_map_manager->CreateOrUpdateZoneMapForTileGroup(table, tg->GetTileGroupId(), nullptr);
                table->DeleteTileGroup(offset);
                tg.reset();
            }

        }
        table->CompactTgList();

////        std::vector<oid_t> tiles_group_id;
////        tiles_group_id.push_back(43);

////        std::vector<oid_t> col_index_list;
////        col_index_list.push_back(1);
////        col_index_list.push_back(2);
////        col_index_list.push_back(5);

////    auto temp_table = GetColdData(33554540, tiles_group_id, col_index_list);

////    std::cout << "TUPLE_COUNT_IN_TEMP: " << temp_table.GetTupleCount() << std::endl;

////    oid_t found_tile_group_count = temp_table.GetTileGroupCount();

////    for (oid_t tile_group_itr = 0; tile_group_itr < found_tile_group_count;
////         tile_group_itr++) {
////      auto tile_group = temp_table.GetTileGroup(tile_group_itr);
////      auto tile_count = tile_group->GetTileCount();

////      for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
////        storage::Tile *tile = tile_group->GetTile(tile_itr);
////        if (tile == nullptr) continue;
////        storage::Tuple tuple(tile->GetSchema());
////        storage::TupleIterator tuple_itr(tile);
////        while (tuple_itr.Next(tuple)) {
////            for (auto i = 0U; i < col_index_list.size(); i++) {
////                auto tupleVal = tuple.GetValue(i);
////                std::cout << tupleVal << std::endl;
////            }
////        }

////      }

////    }

    }

void SerializeMap(std::shared_ptr<storage::TileGroup> *tg) {
    CopySerializeOutput output;
    FileHandle f;
    OutputBuffer *bf = new OutputBuffer();

    FileUtil::OpenWriteFile((DIR_GLOBAL + std::to_string((*tg)->GetTableId()) + "/" +
                        std::to_string((*tg)->GetTileGroupId()) + "_h").c_str(), "wb", f);

    for (const auto &mapping : (*tg)->GetColumnMap()) {
        output.WriteInt(mapping.first);
        output.WriteInt(mapping.second.first);
        output.WriteInt(mapping.second.second);
    }

    bf->WriteData(output.Data(), output.Size());
    uint writesize = bf->GetSize() / getpagesize();

    ssize_t res = write(f.fd, (const void *) (bf->GetData()), (writesize+1) * getpagesize());
    if (res != (ssize_t)((writesize+1) * getpagesize())){
        throw std::logic_error(strerror(errno));
    }
    //  Call fsync
    FileUtil::FFlushFsync(f);
    FileUtil::CloseWriteFile(f);
    delete bf;
}

column_map_type DeserializeMap(oid_t table_id, oid_t tg_id) {
    FileHandle f;
    FileUtil::OpenReadFile((DIR_GLOBAL + std::to_string(table_id) + "/" +
                        std::to_string(tg_id) + "_h").c_str(), f);

    auto table = catalog::Catalog::GetInstance()->GetTableWithOid(
        16777316, table_id);
    auto schema = table->GetSchema();
    auto column_count = schema->GetColumnCount();
    size_t buf_size = column_count * 3 * 4;
    char* buffer;
    int k = posix_memalign((void**) &buffer, getpagesize(), 4096 );
    if(k < 0){
        throw std::logic_error(strerror(errno));
    }
    FileUtil::ReadNBytesFromFile(f,  buffer, 4096);

    CopySerializeInput input_decode((const void *) buffer, buf_size);

    column_map_type map_recovered;

    for (auto i = 0U; i < column_count; ++i) {
        oid_t col_id = (oid_t) input_decode.ReadInt();
        oid_t til_id = (oid_t) input_decode.ReadInt();
        oid_t offset = (oid_t) input_decode.ReadInt();

        map_recovered[col_id] = std::make_pair(til_id, offset);
    }

    FileUtil::CloseReadFile(f);

    return map_recovered;

}
//col_index_list have to be in ascending order
storage::TempTable Evicter::GetColdData(oid_t table_id, const std::vector<oid_t> &tiles_group_id, const std::vector<oid_t> &col_index_list) {
    auto table = catalog::Catalog::GetInstance()->GetTableWithOid(
        16777316, table_id);
    auto schema = table->GetSchema();
    auto temp_schema = catalog::Schema::CopySchema(schema, col_index_list);
//    //ver qual oid                                              //, table->GetLayoutType()
    storage::TempTable temp_table(INVALID_OID, temp_schema, true);

   // char* num_col_buf;//sizeof(int32_t)
   // posix_memalign((void**) &num_col_buf, getpagesize(), getpagesize());

    size_t buf_size = 512*1024;
    char* buffer;
    int k = posix_memalign((void**) &buffer, getpagesize(), buf_size);
    if(k < 0){
        throw std::logic_error(strerror(errno));
    }
//    }
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

        for (uint tuple_count = 0; tuple_count < 500; tuple_count++) {
            std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(temp_schema, true));

            recovered_tuples.push_back(std::move(tuple));
        }

        for (auto tile_it : tiles_to_recover) {
            auto tile_id = tile_it.first;
            auto cols_offsets = tile_it.second;
            FileHandle f;
            FileUtil::OpenReadFile((DIR_GLOBAL + std::to_string(table_id) + "/" +
                                std::to_string(tg_id) + "_" +
                                std::to_string(tile_id)).c_str(), f);
            int filecounter = 4096;

            FileUtil::ReadNBytesFromFile(f, buffer, 4096);
            CopySerializeInput num_col_decode((const void *) buffer, 4);

            oid_t num_col = num_col_decode.ReadInt();
            filecounter -= 4;
            for (oid_t tuple_count = 0; tuple_count < 500; tuple_count++) {
                if(filecounter<=0){
                    FileUtil::ReadNBytesFromFile(f, buffer, 4096);
                    filecounter = 4096;
                }
                CopySerializeInput tuple_decode((const void *) buffer, num_col * 4);

                oid_t offset_current = 0;

                for (oid_t i = 0; i < cols_offsets.size(); i++) {
                    auto col_oid = cols_offsets[i].first;
                    auto offset = cols_offsets[i].second;

                    //pulando
                    while (offset_current < offset) {
                        tuple_decode.ReadInt();
                                    filecounter -= 4;
                        offset_current++;
                    }

                    if (offset_current == offset) {
                        type::Value val = type::Value::DeserializeFrom(
                                    tuple_decode, temp_schema->GetColumn(col_oid).GetType());
                        filecounter -= 4;
                        offset_current++;
                     //   LOG_DEBUG("VALUE RETRIEVED: %d", val.GetAs<int>());
                        recovered_tuples[tuple_count]->SetValue(col_oid, val);
                    } else {
                        std::cout << "ERRORRRRRR!!!!!!! offset_current > offset";
                    }

                }

            }

            FileUtil::CloseReadFile(f);

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

            FileUtil::OpenWriteFile((DIR_GLOBAL + std::to_string((*tg)->GetTableId()) + "/" +
                                std::to_string((*tg)->GetTileGroupId()) + "_" + std::to_string(offset)).c_str(), "wb", f);
            bf->WriteData(output.Data(), output.Size());

            uint writesize = bf->GetSize() / getpagesize();
            ssize_t k = write(f.fd, (const void *) (bf->GetData()), (writesize+1) * getpagesize());
            if(k !=(ssize_t)(writesize+1) * getpagesize()){
                throw std::logic_error(strerror(errno));
            }

            //  Call fsync
                FileUtil::FFlushFsync(f);
                FileUtil::CloseWriteFile(f);
                bf->Reset();
                output.Reset();
        }
        delete bf;
    }

}
}
