//
// Created by root on 28/08/18.
//

#include "MerkleTree.h"

inline uint64_t xxhash(std::pair<hobject_t, eversion_t> pair) {
  return XXH64(&pair, sizeof(pair), XXSEED);
}

void MerkleTree::build_tree() {
  //reduce the leaves by building towards the root
  auto tmp_leaves = new MerkleNode*[leaf_len];
  std::memcpy(tmp_leaves, leaf_arr, leaf_len * sizeof(MerkleNode*));

  for (size_t stride = 2; stride <= leaf_len; stride <<= 1) {
    for (size_t i = 0; i < leaf_len; i += stride) {
      tmp_leaves[i] = new MerkleNode(tmp_leaves[i], tmp_leaves[i + stride/2]);
    }
  }

  root = tmp_leaves[0];
  built = true;

  delete[] tmp_leaves;
}

std::string MerkleTree::print_tree(MerkleNode* node) {
  string result;
  if(node) {
    result += print_tree(node->left);
    result += to_string(node->value) + ",";
    result += print_tree(node->right);
  }

  return result;
}

void MerkleTree::update_obj(
        const hobject_t& hobj,
        const eversion_t& v_curr,
        eversion_t& v_prev,
        MerkleTree::action_t& act)
{
  MerkleNode* node = root;

  int obj_pos = hobj.get_hash() >> reduce_bits;

  uint64_t hash1 = (act != action_t::DELETE) ?
          xxhash(std::pair(hobj, v_curr)) : 0;
  uint64_t hash2 = (act != action_t::CREATE) ?
          xxhash(std::pair(hobj, v_prev)) : 0;
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

void MerkleTree::compare(MerkleTree& other,
                         deque<pair<hobject_t, hobject_t> >& skip_ranges) {
  skip_ranges.clear();
  if(this->root->value == other.root->value) {
    return;
  }

  std::deque<std::pair<uint,uint> > range_diff;
  std::deque<MerkleNode*> lst1;
  std::deque<MerkleNode*> lst2;
  lst1.push_back(this->root->left);
  lst1.push_back(this->root->right);
  lst2.push_back(other.root->left);
  lst2.push_back(other.root->right);
  range_diff.emplace_back(0, leaf_len-1);

  bool isLeftChild = false;
  while (!lst1.empty()) {
    isLeftChild = !isLeftChild;
    auto curr1 = lst1.front();
    lst1.pop_front();
    auto curr2 = lst2.front();
    lst2.pop_front();

    if (curr1->value != curr2->value) {
      auto top = range_diff.front();
      if (curr1->left) { //if left child exists, right child also exists, because of full tree
        lst1.push_back(curr1->left);
        lst1.push_back(curr1->right);
        lst2.push_back(curr2->left);
        lst2.push_back(curr2->right);

        uint m = (top.first + top.second) / 2;
        range_diff.emplace_back(
                isLeftChild ? top.first : m + 1,
                isLeftChild ? m : top.second
        );
      } else {
        uint val = isLeftChild ? top.first : top.second;
        hobject_t lmin, lmax;
        lmin.set_hash(val << reduce_bits); //map 0-X back to 0-UINT32_MAX hash range
        lmax.set_hash(((val+1)<<reduce_bits) - 1);
        skip_ranges.emplace_back(std::move(lmin), std::move(lmax));
      }
    }
    if (!isLeftChild) {
      range_diff.pop_front();
    }
  }

  //merge continuous ranges
  auto it = skip_ranges.begin();
  while(it != skip_ranges.end()) {
    auto next = it+1;

    if(it->second == next->first) {
      it->second = next->second;
      skip_ranges.erase(next);
    } else {
      ++it;
    }
  }

  //create skip ranges
  auto prev_end = skip_ranges.end();
  hobject_t max = hobject_t(hobject_t::get_max());
  skip_ranges.emplace_back(hobject_t(), max);
  if (skip_ranges.empty()) return;
  else {
    for (auto &pair : skip_ranges) {
      skip_ranges.end()->second = std::move(pair.first);
      skip_ranges.emplace_back(pair.second, max);
    }
  }
  //remove include ranges
  skip_ranges.erase(skip_ranges.begin(), prev_end);
}

void MerkleNode::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl)
  encode(value, bl);
  ENCODE_FINISH(bl)
}

void MerkleNode::decode(bufferlist::const_iterator &bl) {
  DECODE_START(1, bl)
  decode(value, bl);
  DECODE_FINISH(bl)
}

void MerkleTree::encode(bufferlist &bl) const {
  std::list<MerkleNode*> queue;
  std::vector<decltype(MerkleNode::value)> v;

  v.reserve(size());

  queue.push_back(root);
  while(!queue.empty()) {
    auto node = queue.front(); queue.pop_front();
    v.push_back(node->value);
    if (node->left) {
      queue.push_back(node->left);
      queue.push_back(node->right);
    }
  }

  ENCODE_START(1, 1, bl)
  encode(leaf_len, bl);
  encode(tree_depth, bl);
  encode(reduce_bits, bl);
  encode(size(), bl);
  encode(v, bl);
  ENCODE_FINISH(bl)
}

void MerkleTree::decode(bufferlist::const_iterator &bl) {
  std::vector<decltype(MerkleNode::value)> tmp;
  std::vector<MerkleNode*> retain;
  size_t size;

  DECODE_START(1, bl)
  decode(leaf_len, bl);
  decode(tree_depth, bl);
  decode(reduce_bits, bl);
  decode(size, bl);
  decode(tmp, bl);

  retain.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    retain[i] = new MerkleNode(tmp[i]);
  }
  tmp.clear();

  init(size);
  root = retain[0];

  for (size_t i = 0; i < size; ++i) {
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
  DECODE_FINISH(bl)
}

void MerkleTree::update_leaf(std::pair<hobject_t, eversion_t> ov_pair) {
  leaf_arr[ov_pair.first.get_hash() >> reduce_bits]->value ^= xxhash(ov_pair);
}
