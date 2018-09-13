//
// Created by root on 28/08/18.
//

#ifndef CEPH_MERKLETREE_H
#define CEPH_MERKLETREE_H

#include <cstdint>
#include <utility>
#include <list>
#include "include/buffer.h"
#include "xxHash/xxhash.h"
#include "common/hobject.h"
#include "osd/osd_types.h"


#define XXSEED 0

inline uint64_t join_hash(uint64_t l, uint64_t r) {
  uint64_t joined[2] = { l, r };
  return XXH64(&joined, sizeof(joined), XXSEED);
}

class MerkleNode {
public:
  uint64_t value;
  MerkleNode* left;
  MerkleNode* right;

  MerkleNode() = default;
  MerkleNode(MerkleNode& n) = default;

  MerkleNode(MerkleNode* l, MerkleNode* r) : left(l), right(r) {
    this->value = join_hash(l->value, r->value);
  }

  ~MerkleNode() {
    if (left) delete left;
    if (right) delete right;
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator &bl);
};
WRITE_CLASS_ENCODER(MerkleNode)

class MerkleTree {
private:
  size_t leaf_len; //must be a power of 2
  size_t tree_depth;
  size_t reduce_bits;
  bool built = false;

  MerkleNode* root;
  MerkleNode** leaf_arr;
  
  void init(size_t len) {
    leaf_len = len;
    leaf_arr = new MerkleNode*[len];
    for (int i = 0; i < len; ++i) {
      leaf_arr[i] = new MerkleNode;
    }
    tree_depth = static_cast<size_t>(log2(len));
    reduce_bits = sizeof(uint32_t) * 8 - tree_depth;
    root = nullptr;
  }

public:
  enum action_t {
    CREATE,
    MODIFY,
    DELETE
  };

  MerkleTree() = default;
  explicit MerkleTree(size_t len) {
    init(len);
  }

  ~MerkleTree() {
    delete root;
  }

  MerkleNode* get_root() {
    return root;
  }

  void build_tree();
  void update_tree(std::vector<ghobject_t>* objects);
  void update_obj(hobject_t* obj, eversion_t curr, eversion_t prev,
          MerkleTree::action_t act);
  void compare(MerkleTree* other, std::vector<uint>* diff);
  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator &bl);
  
  size_t size() const {
    return leaf_len*2-1;
  }
};
WRITE_CLASS_ENCODER(MerkleTree)


#endif //CEPH_MERKLETREE_H
