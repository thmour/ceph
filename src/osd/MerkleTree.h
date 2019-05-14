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
  uint64_t value = 0;
  MerkleNode* left = 0;
  MerkleNode* right = 0;

  MerkleNode() = default;
  MerkleNode(const MerkleNode& n) = default;
  MerkleNode(uint64_t value) {
    this->value = value;
  }

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
    for (size_t i = 0; i < len; ++i) {
      leaf_arr[i] = new MerkleNode;
    }
    tree_depth = static_cast<size_t>(log2(len));
    reduce_bits = sizeof(uint32_t) * 8 - tree_depth; //hobject::hash --> uint32_t
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
  void update_leaf(std::pair<hobject_t, eversion_t> ov_pair);
  std::string print_tree(MerkleNode* node);
  void update_obj(const hobject_t&, const eversion_t&, eversion_t&,
          MerkleTree::action_t&);
  void compare(MerkleTree& other, deque<pair<hobject_t, hobject_t> >& skip_ranges);
  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator &bl);

  size_t size() const {
    return leaf_len*2-1;
  }

  bool is_built() {
    return built;
  }
};
WRITE_CLASS_ENCODER(MerkleTree)

#endif //CEPH_MERKLETREE_H
