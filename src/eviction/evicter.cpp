#include "eviction/evicter.h"
#include "type/serializeio.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "util/file_util.h"
#include "storage/tile.h"
#include <string>

#include "util/output_buffer.h"
#include "gc/gc_manager_factory.h"

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

    void Evicter::EvictTileGroup(std::shared_ptr<storage::TileGroup> *tg) {
        CopySerializeOutput output;
        FileHandle f;
        OutputBuffer *bf = new OutputBuffer();

        for (uint offset = 0; offset < (*tg)->GetTileCount(); offset++) {
            auto tile = (*tg)->GetTile(offset);

            tile->SerializeTo(output, (*tg)->GetActiveTupleCount());
        }

        if (!FileUtil::CreateFile((DIR_GLOBAL + std::to_string((*tg)->GetTableId()) + "/" +
                                   std::to_string((*tg)->GetTileGroupId())).c_str()))
            LOG_DEBUG("ERROR - CREATE FILE");

        FileUtil::OpenFile((DIR_GLOBAL + std::to_string((*tg)->GetTableId()) + "/" +
                            std::to_string((*tg)->GetTileGroupId())).c_str(), "wb", f);
        bf->WriteData(output.Data(), output.Size());

        fwrite((const void *) (bf->GetData()), bf->GetSize(), 1, f.file);

        //  Call fsync
            FileUtil::FFlushFsync(f);
            FileUtil::CloseFile(f);
            delete bf;
    }

}
}
