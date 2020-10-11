import struct
import base64

# constants
ITEM_HEADER_MAGIC = struct.pack("I", 11223344)
ITEM_HEADER_MAGIC_AND_VERSION = ITEM_HEADER_MAGIC + struct.pack("I", 1)

OPERAND_TYPE_LONG = 259
OPERAND_TYPE_DOUBLE = 261


def encode_list(list_value):
    typecode = 'l'
    if len(list_value) and isinstance(list_value[0], float):
        typecode = 'd'

    return encode_array(list_value, typecode)


def encode_array(array_value, typecode):
    num_items = len(array_value)
    operand_type = OPERAND_TYPE_LONG if typecode == 'l' else OPERAND_TYPE_DOUBLE

    encoded_array = ITEM_HEADER_MAGIC_AND_VERSION + struct.pack('II' + typecode * num_items,
                                                                num_items * 8,
                                                                operand_type,
                                                                *array_value)

    return base64.b64encode(encoded_array)


def decode(encoded_array):
    static_header_len = len(ITEM_HEADER_MAGIC_AND_VERSION)

    # do a quick peek before we decode
    if len(encoded_array) <= static_header_len or not encoded_array.startswith(ITEM_HEADER_MAGIC_AND_VERSION):
        raise ValueError('Not an encoded array')

    # get header (which contains number of items and type
    header = encoded_array[static_header_len:static_header_len + 8]
    values = encoded_array[static_header_len + len(header):]

    # unpack the header to get the size and operand
    unpacked_header = struct.unpack('II', header)

    # get the typecode and number of items
    typecode = 'l' if unpacked_header[1] == OPERAND_TYPE_LONG else 'd'
    num_items = int(unpacked_header[0] / 8)

    # decode the values
    return list(struct.unpack(typecode * num_items, values))
