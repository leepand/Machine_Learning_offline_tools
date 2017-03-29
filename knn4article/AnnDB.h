#ifndef _ANN_DB_H_
#define _ANN_DB_H_

#include "common_utils.h"
#include "annoylib.h"
#include <vector>
#include <glog/logging.h>

/**
 * @brief 
 *
 * @tparam IdType   unsigned integer
 * @tparam DataType float
 */
template <typename IdType, typename DataType>
class AnnDB {
public:
    typedef AnnoyIndex< IdType, DataType, Angular, RandRandom > WordAnnIndex;
    static const IdType                                         INVALID_ID = (IdType)-1;

public:
    explicit AnnDB( int nFields )
                : m_nFields(nFields)
                , m_AnnIndex(nFields) {}

    virtual ~AnnDB() = default;

    /**
     * @brief  建立Annoy索引，参阅 AnnoyIndex::build 
     *         More trees gives higher precision when querying. After calling ``build``, 
     *         no more items can be added.
     * @param q  number of trees
     */
    void buildIndex( int q )
    { 
        DLOG(INFO) << "AnnDB::buildIndex() q = " << q;
        m_AnnIndex.build(q); 
    }

    /**
     * @brief  将建立好的Annoy索引存入文件
     */
    bool saveIndex( const char *filename )
    { return m_AnnIndex.save(filename); }
    /**
     * @brief  从文件中导入AnnoyIndex
     */
    bool loadIndex( const char *filename )
    { return m_AnnIndex.load(filename); }

    DataType getDistance( IdType i, IdType j )
    { return m_AnnIndex.get_distance(i, j); }

    void getVector( IdType i, std::vector<DataType> &ret )
    {
        ret.resize( m_nFields );
        m_AnnIndex.get_item(i, &ret[0]);
    }

    void kNN_By_Id(IdType id, size_t n, 
                    std::vector<IdType> &result, std::vector<DataType> &distances,
                    size_t search_k = (size_t)-1 )
    {
        DLOG(INFO) << "AnnDB::kNN_By_Id() id = " << id << ", n = " << n
                << ", search_k = " << search_k;
        result.clear(); distances.clear();
        // wrapped func use push_back
        // 返回结果已按升序排列
        m_AnnIndex.get_nns_by_item( id, n, search_k, &result, &distances );
    }

    void kNN_By_Vector(const std::vector<DataType> &v, size_t n, 
                    std::vector<IdType> &result, std::vector<DataType> &distances,
                    size_t search_k = (size_t)-1 )
    {
        DLOG(INFO) << "AnnDB::kNN_By_Vector()  n = " << n
                << ", search_k = " << search_k;
        if (v.size() != m_nFields)
            THROW_RUNTIME_ERROR("AnnDB::kNN_By_Vector() input vector size " << v.size()
                    << " not equal to predefined size " << m_nFields);
        result.clear(); distances.clear();
        m_AnnIndex.get_nns_by_vector( &v[0], n, search_k, &result, &distances );
    }

    void addItem( const std::vector<DataType> &v )
    {
        if (v.size() != m_nFields)
            THROW_RUNTIME_ERROR("AnnDB::addItem() input vector size " << v.size()
                    << " not equal to predefined size " << m_nFields);
        IdType id = m_AnnIndex.get_n_items();
        // DLOG(INFO) << "addItem id = " << id;
        m_AnnIndex.add_item( id, &v[0] );
    }

    std::size_t size()
    { return m_AnnIndex.get_n_items(); }

protected:
    WordAnnIndex        m_AnnIndex;
    int                 m_nFields;
};


#endif

