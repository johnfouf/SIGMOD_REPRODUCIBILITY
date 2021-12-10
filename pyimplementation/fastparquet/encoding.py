
"""encoding.py - methods for reading parquet encoded data blocks."""

from __future__ import absolute_import
from __future__ import division
import gc
import array
from itertools import repeat
import numba
import numpy as np
import msgpack
import msgpack_numpy as m
from .thrift_structures import parquet_thrift
from .util import byte_buffer
import struct
from array import array
m.patch()

import zlib


@numba.njit(nogil=True)
def unpack_boolean(data, out):
    for i in range(len(data)):
        d = data[i]
        for j in range(8):
            out[i * 8 + j] = d & 1
            d >>= 1


def read_plain_boolean(raw_bytes, count):
    """Read `count` booleans using the plain encoding."""
    data = np.frombuffer(raw_bytes, dtype='uint8')
    padded = len(raw_bytes) * 8
    out = np.empty(padded, dtype=bool)
    unpack_boolean(data, out)
    return out[:count]


DECODE_TYPEMAP = {
    parquet_thrift.Type.INT32: np.int32,
    parquet_thrift.Type.INT64: np.int64,
    parquet_thrift.Type.INT96: np.dtype('S12'),
    parquet_thrift.Type.FLOAT: np.float32,
    parquet_thrift.Type.DOUBLE: np.float64,
}


def read_plain(raw_bytes,global_dictionary,filters, type_, count, width=0):

    type = 'i'
    ind = list(struct.unpack(type, raw_bytes[0:4]))
    if ind[0]>0:
        return read_plain_diff(raw_bytes, global_dictionary,filters, type_, count, width=0)
    else:
        return read_plain_parquet(raw_bytes, global_dictionary,filters, type_, count, width=0)
   
def read_plain_parquet(raw_bytes, global_dictionary,filters, type, count, width=0):
    type1 = 'i'*7
    ind = list(struct.unpack(type1, raw_bytes[0:28]))
    
    search_filter = []
    search_ops = []
    search = 0
    type_ = type.type
    for filter in filters:
      try:
        if filter[0] == type.path_in_schema[0]:
            search_filter.append((filter[1],filter[2]))
            search = 1
      except:
        pass
            
    
    if type_ in DECODE_TYPEMAP:
        dtype = DECODE_TYPEMAP[type_]
        return np.frombuffer(byte_buffer(raw_bytes), dtype=dtype, count=count)
    if type_ == parquet_thrift.Type.FIXED_LEN_BYTE_ARRAY:
        if count == 1:
            width = len(raw_bytes)
        dtype = np.dtype('S%i' % width)
        return np.frombuffer(byte_buffer(raw_bytes), dtype=dtype, count=count)
    if type_ == parquet_thrift.Type.BOOLEAN:
        return read_plain_boolean(raw_bytes, count)
    # variable byte arrays (rare)
    try:
    	if search == 0:
            return msgpack.loads(zlib.decompress(raw_bytes[28:28+ind[1]]))
    	if search_filter[0][0] == '==' or search_filter[0][0] == '=': 
            vv = numpy.array(msgpack.loads(zlib.decompress(raw_bytes[28:28+ind[1]])), dtype = 'O')
            return numpy.array(np.where(vv == search_filter[0][1], vv, b''),dtype = 'O')
            #for i in range(len(vv)):
            #    print (vv[i], search_filter[0][1])
            #    if vv[i] != search_filter[0][1]: vv[i] = b''
            #return vv
           
    
    	vv = np.array(unpack_byte_array(raw_bytes, count), dtype='O')
    	
    	if search_filter[0][0] == '>=': return np.where(vv >= search_filter[0][1], vv, None)
    	elif search_filter[0][0] == '>': return np.where(vv > search_filter[0][1], vv, None)
    	elif search_filter[0][0] == '<=': return np.where(vv <= search_filter[0][1], vv, None)
    	elif search_filter[0][0] == '<': return np.where(vv < search_filter[0][1], vv, None)
    	elif search_filter[0][0] == '!=': return np.where(vv != search_filter[0][1], vv, None)
     
    except RuntimeError:
        if count == 1:
            # e.g., for statistics
            return np.array([raw_bytes], dtype='O')
        else:
            raise

def read_plain_filter(raw_bytes, metadata, count, width=0):
    searchval = "acergy"
    type_ = metadata.type
    if type_ in DECODE_TYPEMAP:
        dtype = DECODE_TYPEMAP[type_]
        return np.frombuffer(byte_buffer(raw_bytes), dtype=dtype, count=count)
    if type_ == parquet_thrift.Type.FIXED_LEN_BYTE_ARRAY:
        if count == 1:
            width = len(raw_bytes)
        dtype = np.dtype('S%i' % width)
        return np.frombuffer(byte_buffer(raw_bytes), dtype=dtype, count=count)
    if type_ == parquet_thrift.Type.BOOLEAN:
        return read_plain_boolean(raw_bytes, count)
    # variable byte arrays (rare)
    try:
        return np.where(np.array(unpack_byte_array(raw_bytes, count), dtype='O')!=searchval, None, searchval)
    except RuntimeError:
        if count == 1:
            # e.g., for statistics
            return np.array([raw_bytes], dtype='O')
        else:
            raise

def read_plain2(raw_bytes, metadata, count, width=0):
    type_ = metadata.type
    if type_ in DECODE_TYPEMAP:
        dtype = DECODE_TYPEMAP[type_]
        return np.frombuffer(byte_buffer(raw_bytes), dtype=dtype, count=count)
    if type_ == parquet_thrift.Type.FIXED_LEN_BYTE_ARRAY:
        if count == 1:
            width = len(raw_bytes)
        dtype = np.dtype('S%i' % width)
        return np.frombuffer(byte_buffer(raw_bytes), dtype=dtype, count=count)
    if type_ == parquet_thrift.Type.BOOLEAN:
        return read_plain_boolean(raw_bytes, count)
    # variable byte arrays (rare)
    try:
        
        return np.array(unpack_byte_array(raw_bytes, count), dtype='O')
    except RuntimeError:
        if count == 1:
            # e.g., for statistics
            return np.array([raw_bytes], dtype='O')
        else:
            raise


def read_plain_dict(raw_bytes, type_, count, width=0):
    if type_ in DECODE_TYPEMAP:
        dtype = DECODE_TYPEMAP[type_]
        return np.frombuffer(byte_buffer(raw_bytes), dtype=dtype, count=count)
    if type_ == parquet_thrift.Type.FIXED_LEN_BYTE_ARRAY:
        if count == 1:
            width = len(raw_bytes)
        dtype = np.dtype('S%i' % width)
        return np.frombuffer(byte_buffer(raw_bytes), dtype=dtype, count=count)
    if type_ == parquet_thrift.Type.BOOLEAN:
        return read_plain_boolean(raw_bytes, count)
    # variable byte arrays (rare)
    try:
        
        type = '!'+'i'*3
        ind = list(struct.unpack(type, raw_bytes[0:16]))
        values = msgpack.loads(raw_bytes[16:16+ind[0]])
        type1 = '<'+'H'*ind[2]
        
        offsets = list(struct.unpack(type1, raw_bytes[16+ind[0]:16+ind[0]+ind[1]]))
        return map(values.__getitem__, offsets)
    except RuntimeError:
        if count == 1:
            # e.g., for statistics
            return np.array([raw_bytes], dtype='O')
        else:
            raise


def read_plain3(raw_bytes, global_dictionary,filters, type_, count, width=0):
    search_filter = []
    search_ops = []
    search = 0
    
    
    for filter in filters:
        if filter[0] == type_.path_in_schema[0]:
            search_filter.append((filter[1],filter[2]))
            search = 1
    
    if search == 0:
        return read_plain_no_filters(raw_bytes, global_dictionary,filters, type_, count, width)
            
    

    type = '!'+'i'*5
    ind = list(struct.unpack(type, raw_bytes[0:20]))


    
    if ind[0]>34 and (search_filter[0][0] != "==" or (global_dictionary[0][1].size == 0 and search_filter[0][0] == "==")):


        values = msgpack.unpackb(raw_bytes[20:20+ind[0]],object_hook=m.decode)
        print(values)
        global_dictionary[0][0].extend(values)
        
    if global_dictionary[0][1].size == 0:
        global_dictionary[0][0].extend([None]*ind[4])
        global_dictionary[0][2] += ind[4]
        
        return []
    else:
        if search_filter[0][0] != "==" and bika == 0 :
            global_dictionary[0][0].extend([None]*ind[4])
        type2 = np.int32 if ind[3]==0 else np.int16 if ind[3] == 1 else np.int8
        type1 = 'i' if ind[3]==0 else 'H' if ind[3] == 1 else 'B'
        global_dictionary[0][2] += ind[4]
        
        #np.where(a < max(global_dictionary[1]), None, searchval)
        
        #a = np.frombuffer(raw_bytes[20+ind[0]:20+ind[0]+ind[1]], dtype=type2)
        #ret = [None]*len(a)
        #for i,x in enumerate(a):
        #    if x in global_dictionary[1]:
        #        ret[i] = global_dictionary[0][x]
        #return ret
        
        a = np.where(global_dictionary[0][0] <= search_filter[0][1], global_dictionary[0][0], None)
        return map(a.__getitem__, array(type1, raw_bytes[20+ind[0]:20+ind[0]+ind[1]]))
        #return np.where(a in list(global_dictionary[1].any(), "lala",None)
    

def read_plain_diff(raw_bytes, global_dictionary,filters, type_, count, width=0):
    search_filter = []
    search_ops = []
    search = 0
    try:
        x = int(filters[len(filters)-1])
    except:
        filters.append(-1)
        filters.append(0)
        
    for filter in filters:
      try:
        if filter[0] == type_.path_in_schema[0]:
            search_filter.append((filter[1],filter[2]))
            search = 1
      except:
        pass
    
    
    if search == 0:
        return read_plain_no_filters(raw_bytes, global_dictionary,filters, type_, count, width)
    if search_filter[0][0] == '==' or search_filter[0][0] == '=':
        return read_plain_equal(raw_bytes, global_dictionary,search_filter, type_, count, filters, width)
    else:
        return read_plain_range(raw_bytes, global_dictionary,search_filter, type_, count, width)

    type = '!'+'i'*5
    ind = list(struct.unpack(type, raw_bytes[0:20]))
    if ind[0]>1:
        values = msgpack.unpackb(raw_bytes[20:20+ind[0]],object_hook=m.decode)
        global_dictionary[0][0].extend(values)
    


    type1 = 'i' if ind[3]==0 else 'H' if ind[3] == 1 else 'B'
    l = np.array(global_dictionary[0][0])
    a = np.where(l == search_filter[0][1], l, None)
    
    return map(a.__getitem__, array(type1, raw_bytes[20+ind[0]:20+ind[0]+ind[1]]))




def read_plain_range(raw_bytes, global_dictionary,searchfilter, type_, count, width=0):
    type = '!'+'i'*5
    ind = list(struct.unpack(type, raw_bytes[0:20]))

    if ind[0]>34:
        values = msgpack.unpackb(raw_bytes[20:20+ind[0]],object_hook=m.decode)
        if searchfilter[0][0] == '>=': filtered = np.where(values >= searchfilter[0][1],values,None)
        elif searchfilter[0][0] == '>': filtered = np.where(values > searchfilter[0][1],values,None)
        elif searchfilter[0][0] == '<=': filtered = np.where(values <= searchfilter[0][1],values,None)
        elif searchfilter[0][0] == '<': filtered = np.where(values < searchfilter[0][1],values,None)
        elif searchfilter[0][0] == '!=': filtered = np.where(values != searchfilter[0][1],values,None)
        
        
        global_dictionary[0][0].extend(filtered)
    

    type1 = 'i' if ind[3]==0 else 'H' if ind[3] == 1 else 'B'
    return map(global_dictionary[0][0].__getitem__, array(type1, raw_bytes[20+ind[0]:20+ind[0]+ind[1]]))
    
def read_plain_equal(raw_bytes, global_dictionary,searchfilter, type_, count, filters, width=0):
    
    type = 'i'*8
    ind = list(struct.unpack(type, raw_bytes[0:32]))
    offset = filters[len(filters)-2]
    if offset == -1:
      if ind[0]>34 and global_dictionary[0][1].size == 0:
        values = msgpack.loads(raw_bytes[32+ind[6]+ind[7]*2:32+ind[6]+ind[7]*2+ind[0]])
        lenvals = len(values)
        
        for i in range(lenvals):
            if values[i] == searchfilter[0][1]:
                offset = i + filters[len(filters)-1]
                filters[len(filters)-2] = offset
                break
        filters[len(filters)-1]+=lenvals
    if offset == -1:
        return [b'']*ind[2]
    else:
        type1 = 'i' if ind[3]==0 else 'H' if ind[3] == 1 else 'B'
        type2 = np.uint32 if ind[3]==0 else np.uint16 if ind[3] == 1 else np.uint8
        return np.array((np.where(np.frombuffer(raw_bytes[32+ind[0]+ind[6]+ind[7]*2:32+ind[0]+ind[6]+ind[7]*2+ind[1]], dtype=type2) == offset, searchfilter[0][1], b'')),dtype='O')
        #return [searchfilter[0][1] if x == offset else b'' for x in list(array(type1, raw_bytes[32+ind[0]+ind[6]+ind[7]*2:32+ind[0]+ind[6]+ind[7]*2+ind[1]]))]
        #for i,j in enumerate(array(type1, raw_bytes[32+ind[0]+ind[6]+ind[7]*2:32+ind[0]+ind[6]+ind[7]*2+ind[1]])):
        #   if (j == offset):
        #        ret[i] = searchfilter[0][1]
        #return ret
        
        
    

import numpy
def read_plain_no_filters(raw_bytes, global_dictionary, filters, type_, count, width=0):

    type = 'i'*8
    ind = list(struct.unpack(type, raw_bytes[0:32]))
    if ind[0]>0:
        values = msgpack.loads(raw_bytes[32+ind[6]+ind[7]*2:32+ind[6]+ind[7]*2+ind[0]])
        print(values)
        #for i in range(len(values)):
        #     values[i] = values[i].encode('utf-8')
        global_dictionary[0][0].extend(values)

    type1 = 'i' if ind[3]==0 else 'H' if ind[3] == 1 else 'B'
    lala = map(global_dictionary[0][0].__getitem__, array(type1, raw_bytes[32+ind[0]+ind[6]+ind[7]*2:32+ind[0]+ind[6]+ind[7]*2+ind[1]]))
    
    return lala
    



@numba.jit(nogil=True)
def read_unsigned_var_int(file_obj):  # pragma: no cover
    """Read a value using the unsigned, variable int encoding.
    file-obj is a NumpyIO of bytes; avoids struct to allow numba-jit
    """
    result = 0
    shift = 0
    while True:
        byte = file_obj.read_byte()
        result |= ((byte & 0x7F) << shift)
        if (byte & 0x80) == 0:
            break
        shift += 7
    return result


@numba.njit(nogil=True)
def read_rle(file_obj, header, bit_width, o):  # pragma: no cover
    """Read a run-length encoded run from the given fo with the given header and bit_width.

    The count is determined from the header and the width is used to grab the
    value that's repeated. Yields the value repeated count times.
    """
    count = header >> 1
    width = (bit_width + 7) // 8
    zero = np.zeros(4, dtype=np.int8)
    data = file_obj.read(width)
    zero[:len(data)] = data
    value = zero.view(np.int32)
    o.write_many(value, count)


@numba.njit(nogil=True)
def width_from_max_int(value):  # pragma: no cover
    """Convert the value specified to a bit_width."""
    for i in range(0, 64):
        if value == 0:
            return i
        value >>= 1


@numba.njit(nogil=True)
def _mask_for_bits(i):  # pragma: no cover
    """Generate a mask to grab `i` bits from an int value."""
    return (1 << i) - 1


@numba.njit(nogil=True)
def read_bitpacked(file_obj, header, width, o):  # pragma: no cover
    """
    Read values packed into width-bits each (which can be >8)

    file_obj is a NumbaIO array (int8)
    o is an output NumbaIO array (int32)
    """
    num_groups = header >> 1
    count = num_groups * 8
    byte_count = (width * count) // 8
    raw_bytes = file_obj.read(byte_count)
    mask = _mask_for_bits(width)
    current_byte = 0
    data = raw_bytes[current_byte]
    bits_wnd_l = 8
    bits_wnd_r = 0
    total = byte_count * 8
    while total >= width:
        # NOTE zero-padding could produce extra zero-values
        if bits_wnd_r >= 8:
            bits_wnd_r -= 8
            bits_wnd_l -= 8
            data >>= 8
        elif bits_wnd_l - bits_wnd_r >= width:
            o.write_byte((data >> bits_wnd_r) & mask)
            total -= width
            bits_wnd_r += width
        elif current_byte + 1 < byte_count:
            current_byte += 1
            data |= (raw_bytes[current_byte] << bits_wnd_l)
            bits_wnd_l += 8


@numba.njit(nogil=True)
def read_rle_bit_packed_hybrid(io_obj, width, length=None, o=None):  # pragma: no cover
    """Read values from `io_obj` using the rel/bit-packed hybrid encoding.

    If length is not specified, then a 32-bit int is read first to grab the
    length of the encoded data.

    file-obj is a NumpyIO of bytes; o if an output NumpyIO of int32
    """
    if length is None:
        length = read_length(io_obj)
    start = io_obj.loc
    while io_obj.loc-start < length and o.loc < o.len:
        header = read_unsigned_var_int(io_obj)
        if header & 1 == 0:
            read_rle(io_obj, header, width, o)
        else:
            read_bitpacked(io_obj, header, width, o)
    io_obj.loc = start + length
    return o.so_far()


@numba.njit(nogil=True)
def read_length(file_obj):  # pragma: no cover
    """ Numpy trick to get a 32-bit length from four bytes

    Equivalent to struct.unpack('<i'), but suitable for numba-jit
    """
    sub = file_obj.read(4)
    return sub[0] + sub[1]*256 + sub[2]*256*256 + sub[3]*256*256*256


class NumpyIO(object):  # pragma: no cover
    """
    Read or write from a numpy arra like a file object

    This class is numba-jit-able (for specific dtypes)
    """
    def __init__(self, data):
        self.data = data
        self.len = data.size
        self.loc = 0

    def read(self, x):
        self.loc += x
        return self.data[self.loc-x:self.loc]

    def read_byte(self):
        self.loc += 1
        return self.data[self.loc-1]

    def write(self, d):
        l = len(d)
        self.loc += l
        self.data[self.loc-l:self.loc] = d

    def write_byte(self, b):
        if self.loc >= self.len:
            # ignore attempt to write past end of buffer
            return
        self.data[self.loc] = b
        self.loc += 1

    def write_many(self, b, count):
        self.data[self.loc:self.loc+count] = b
        self.loc += count

    def tell(self):
        return self.loc

    def so_far(self):
        """ In write mode, the data we have gathered until now
        """
        return self.data[:self.loc]

spec8 = [('data', numba.uint8[:]), ('loc', numba.int64), ('len', numba.int64)]
Numpy8 = numba.experimental.jitclass(spec8)(NumpyIO)
spec32 = [('data', numba.uint32[:]), ('loc', numba.int64), ('len', numba.int64)]
Numpy32 = numba.experimental.jitclass(spec32)(NumpyIO)


def _assemble_objects(assign, defi, rep, val, dic, d, null, null_val, max_defi, prev_i):
    """Dremel-assembly of arrays of values into lists

    Parameters
    ----------
    assign: array dtype O
        To insert lists into
    defi: int array
        Definition levels, max 3
    rep: int array
        Repetition levels, max 1
    dic: array of labels or None
        Applied if d is True
    d: bool
        Whether to dereference dict values
    null: bool
        Can an entry be None?
    null_val: bool
        can list elements be None
    max_defi: int
        value of definition level that corresponds to non-null
    prev_i: int
        1 + index where the last row in the previous page was inserted (0 if first page)
    """
    ## TODO: good case for cython
    if d:
        # dereference dict values
        val = dic[val]
    i = prev_i
    vali = 0
    part = []
    started = False
    have_null = False
    if defi is None:
        defi = value_maker(max_defi)
    for de, re in zip(defi, rep):
        if not re:
            # new row - save what we have
            if started:
                assign[i] = None if have_null else part
                part = []
                i += 1
            else:
                # first time: no row to save yet, unless it's a row continued from previous page
                if vali > 0:
                    assign[i - 1].extend(part) # add the items to previous row
                    part = []
                    # don't increment i since we only filled i-1
                started = True
        if de == max_defi:
            # append real value to current item
            part.append(val[vali])
            vali += 1
        elif de > null:
            # append null to current item
            part.append(None)
        # next object is None as opposed to an object
        have_null = de == 0 and null
    if started: # normal case - add the leftovers to the next row
        assign[i] = None if have_null else part
    else: # can only happen if the only elements in this page are the continuation of the last row from previous page
        assign[i - 1].extend(part)
    return i


def value_maker(val):
    while True:
        yield val
