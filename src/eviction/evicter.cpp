#include "eviction/evicter.h"
#include "type/serializeio.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "util/file_util.h"
#include "storage/tile.h"
#include <string>

#include "util/output_buffer.h"
#include "gc/gc_manager_factory.h"

#include "storage/storage_manager.h"

namespace peloton  {
namespace eviction {

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

storage::TempTable Recover(oid_t table_id, oid_t tg_id) { //, const std::vector<oid_t> &index_list
    oid_t col_index = 0; //sera parametro depois

    auto column_map = DeserializeMap(table_id, tg_id); //Ver outro, melhor sera?
    auto table = storage::StorageManager::GetInstance()->GetTableWithOid(
        16777316, table_id);
    auto schema = table->GetSchema();

    std::vector<oid_t> index_list;
    index_list.push_back(col_index);

    auto temp_schema = catalog::Schema::CopySchema(schema, index_list);

    //ver qual oid
    storage::TempTable temp_table(INVALID_OID, temp_schema, true, table->GetLayoutType());

    std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(temp_schema, true));

    tuple->SetValue();

    temp_table.InsertTuple(tuple.get());

    auto pair = column_map[col_index];
    auto tile_id = pair.first;
    auto offset  = pair.second;

    FileHandle f;
    FileUtil::OpenFile((DIR_GLOBAL + std::to_string(table_id) + "/" +
                        std::to_string(tg_id) + "_" +
                        std::to_string(tile_id)).c_str(), "rb", f);



    return temp_table;

//    auto tg = table->GetTileGroupById(tg_id);
    // PELOTON JA FAZ NO TEMPTABLE, BASTA QUE EU PASSE O SCHEMA E LAYOUT CORRETO.
//    if (tg.get() == nullptr) {
//        tg = table->GetTileGroupWithLayout(16777316, tg_id, column_map, 5);
//        table->AddTileGroup(tg);

//      table->AddTileGroupWithOidForRecovery(tg_id);
//      tg = table->GetTileGroupById(tg_id);
//      catalog::Manager::GetInstance().GetNextTileGroupId(); //????
//    }



//    auto column_count = schema->GetColumnCount();
//    size_t buf_size = column_count * 3 * 4;
//    std::unique_ptr<char[]> buffer(new char[buf_size]);

//    FileUtil::ReadNBytesFromFile(f,  buffer.get(), buf_size);

//    CopySerializeInput input_decode((const void *) buffer.get(), buf_size);

//    column_map_type map_recovered;

//    for (auto i = 0U; i < column_count; ++i) {
//        oid_t col_id = (oid_t) input_decode.ReadInt();
//        oid_t til_id = (oid_t) input_decode.ReadInt();
//        oid_t offset = (oid_t) input_decode.ReadInt();

//        map_recovered[col_id] = std::make_pair(til_id, offset);
//    }

//    FileUtil::CloseFile(f);

}

    void Evicter::EvictTileGroup(std::shared_ptr<storage::TileGroup> *tg) {
        CopySerializeOutput output;
        FileHandle f;
        OutputBuffer *bf = new OutputBuffer();

        SerializeMap(tg);

        for (uint offset = 0; offset < (*tg)->GetTileCount(); offset++) {
            auto tile = (*tg)->GetTile(offset);

            tile->SerializeTo(output, (*tg)->GetActiveTupleCount());
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
