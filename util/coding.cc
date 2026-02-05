//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

#include <algorithm>

#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"

namespace ROCKSDB_NAMESPACE {

// conversion' conversion from 'type1' to 'type2', possible loss of data
#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4244)
#endif
/**
 * 底层的编码方式 是序列化/反序列化的基石
 * 定长32位的整数编成变长
 * 对于每个字节 低7位放数据 最高位放标识 1标识后面还有字节
 * 0标识这个最后一个字节后面没有了 小端序的方式
 * 编成一个整数最多会用5个字节
 * @return 编完后的一下位置
 */
char* EncodeVarint32(char* dst, uint32_t v) {
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  // 二进制1000 0000 就是高第7位的标识1 表示后面还有字节
  static const int B = 128;
  if (v < (1 << 7)) {
    // 7bit能放下 就把数据放在抵7位上 最高位0
    // 先解引用把数据写进去再移动指针
    *(ptr++) = v;
  } else if (v < (1 << 14)) {
    // 2字节能放下
    // 第1个字节放下数据的低7位 第1个字节的最高位放1 表示还有后续
    // 第2个字节放数据刨去7位的高位 第2字节最高位放0
    *(ptr++) = v | B;
    *(ptr++) = v >> 7;
  } else if (v < (1 << 21)) {
    // 3个字节能放下
    // 第1个字节低7位放数据的低7位 第1个字节最高位放1表示还有后续
    *(ptr++) = v | B;
    // 第2个字节低7位放数据刨去7位后的低7位 第2个字节最高位放1表示还有后续
    *(ptr++) = (v >> 7) | B;
    // 第3个字节低7位放数据刨去14位后的低7位 第3个字节最高位放0表示没有后续了
    *(ptr++) = v >> 14;
  } else if (v < (1 << 28)) {
    // 4个字节能放下
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = v >> 21;
  } else {
    // 用5个字节编码
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = (v >> 21) | B;
    *(ptr++) = v >> 28;
  }
  return reinterpret_cast<char*>(ptr);
}
#if defined(_MSC_VER)
#pragma warning(pop)
#endif

/**
 * 解码 [p...limit)
 * @param p 要解的低地址
 * @param limit 要解的高地址 右开
 * @param value 解码结果 数字
 * @return 指针指向的是解码数字后的那个地址
 * 也就是说调用方解码到数字后可以继续解码 比如TLV 现在T拿到了 继续就是L
 */
const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                   uint32_t* value) {
  uint32_t result = 0;
  // 编码的时候是小端序编的 解的时候也是小端序
  // 分批拿 拿8位 低7位是数据 高8位是标识
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // 高8位标识是1 表示还有 继续在下一轮for循环拿下一个8位 拿出现在这8位里面的低7位拼到结果里面
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      // 高8位标识是0 标识数字解码结束了
      result |= (byte << shift);
      *value = result;
      // 数字解完了 现在p指向的是数字后面的那个字节 也就是说解码是一边解一边移动指针
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

}  // namespace ROCKSDB_NAMESPACE
