//
// Created by root on 28/08/18.
//

#include "MerkleTree.h"

inline uint64_t xxhash(std::pair<hobject_t, eversion_t> pair) {
  return XXH64(&pair, sizeof(pair), XXSEED);
}

inline uint64_t xxhash(ghobject_t* obj) {
	return XXH64(obj, sizeof(hobject_t) + sizeof(gen_t), XXSEED);
}

void MerkleTree::build_tree() {
  //reduce the leaves by buiding towards the root
  auto tmp_leaves = new MerkleNode*[leaf_len];
  std::memcpy(tmp_leaves, leaf_arr, leaf_len * sizeof(MerkleNode*));

  for (int stride = 2; stride <= leaf_len; stride <<= 1) {
    for (int i = 0; i < leaf_len; i += stride) {
      tmp_leaves[i] = new MerkleNode(tmp_leaves[i], tmp_leaves[i + stride/2]);
    }
  }

  root = tmp_leaves[0];
  built = true;

  delete[] tmp_leaves;
}

void MerkleTree::update_obj(hobject_t* hobj,
        eversion_t v_curr,
        eversion_t v_prev,
        MerkleTree::action_t act) {
  MerkleNode* node = root;

  int obj_pos = hobj->get_hash() >> reduce_bits;

  uint64_t hash1 = (act != action_t::DELETE) ?
          xxhash(std::make_pair(*hobj, v_curr)) : 0;
  uint64_t hash2 = (act != action_t::CREATE) ?
          xxhash(std::make_pair(*hobj, v_prev)) : 0;
  uint64_t update_val = hash1 ^ hash2;

  std::vector<MerkleNode*> update_list;
  update_list.push_back(node);
  for (size_t bit = leaf_len >> 1; bit > 0; bit >>= 1) {
    node = obj_pos & bit ? node->right : node->left;
    update_list.push_back(node);
  }
  update_list[tree_depth]->value ^= update_val;
  for (auto it = ++update_list.rbegin(); it != update_list.rend(); ++it) {
    (*it)->value = join_hash((*it)->left->value, (*it)->right->value);
  }
}

void MerkleTree::compare(MerkleTree* other, std::vector<uint>* diff) {
  if(this->root->value == other->root->value) {
    return;
  }

  std::list<std::pair<uint,uint> > range_diff;
  std::list<MerkleNode*> lst1;
  std::list<MerkleNode*> lst2;
  lst1.push_back(this->root->left);
  lst1.push_back(this->root->right);
  lst2.push_back(other->root->left);
  lst2.push_back(other->root->right);
  range_diff.emplace_back(0, leaf_len-1);

  bool isLeftChild = false;
  while (!lst1.empty()) {
    isLeftChild = !isLeftChild;
    auto curr1 = lst1.front(); lst1.pop_front();
    auto curr2 = lst2.front(); lst2.pop_front();

    if (curr1->value != curr2->value) {
      auto top = range_diff.front();
      if (curr1->left) { //if children exist, no need to check right
        lst1.push_back(curr1->left);
        lst1.push_back(curr1->right);
        lst2.push_back(curr2->left);
        lst2.push_back(curr2->right);

        uint m = (top.first + top.second) / 2;
        range_diff.emplace_back(
            isLeftChild ? top.first : m+1,
            isLeftChild ? m : top.second
        );
      } else {
        uint val = isLeftChild ? top.first : top.second;
        diff->push_back(val);
      }
    }
    if (!isLeftChild) {
      range_diff.pop_front();
    }
  }
}

void MerkleNode::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  encode(value, bl);
  ENCODE_FINISH(bl);
}

void MerkleNode::decode(bufferlist::const_iterator &bl) {
  DECODE_START(1, bl);
  decode(value, bl);
  DECODE_FINISH(bl);
}

void MerkleTree::encode(bufferlist &bl) const {
  std::list<MerkleNode> queue;
  std::vector<MerkleNode> v;

  v.reserve(this->size());

  queue.push_back(*root);
  while(!queue.empty()) {
    auto val = queue.front(); queue.pop_front();
    v.push_back(val);
    if (val.left)
      queue.push_back(*val.left);
    if (val.right)
      queue.push_back(*val.right);
  }

  ENCODE_START(1, 1, bl);
  encode(leaf_len, bl);
  encode(tree_depth, bl);
  encode(reduce_bits, bl);
  encode(this->size(), bl);
  encode(v, bl);
  ENCODE_FINISH(bl);
}

void MerkleTree::decode(bufferlist::const_iterator &bl) {
  std::vector<MerkleNode> tmp;
  std::vector<MerkleNode*> retain;
  size_t size;

  DECODE_START(1, bl);
  decode(leaf_len, bl);
  decode(tree_depth, bl);
  decode(reduce_bits, bl);
  decode(size, bl);
  decode(tmp, bl);

  retain.reserve(size);
  for(int i = 0; i < size; ++i) {
    retain[i] = new MerkleNode(tmp[i]);
  }
  tmp.clear();

  init(size);
  root = retain[0];

  for(int i = 0; i < size; ++i) {
    if(2*i+1 < size)
      retain[i]->left = retain[2*i+1];
    else
      retain[i]->left = nullptr;

    if(2*i+2 < size)
      retain[i]->right = retain[2*i+2];
    else
      retain[i]->right = nullptr;

  }
  root = retain[0];
  DECODE_FINISH(bl);
}

void MerkleTree::update_tree(std::vector<ghobject_t>* ghobjects) {
  //populate leaves
  for (auto obj : *ghobjects) {
    leaf_arr[obj.hobj.get_hash() >> reduce_bits]->value ^= xxhash(&obj);
  }
}
