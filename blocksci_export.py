import blocksci
import pandas as pd


def addr_str(addr_obj):
    if addr_obj.type == blocksci.address_type.multisig:
        res = [x.address_string for x in addr_obj.addresses]
    elif addr_obj.type == blocksci.address_type.nonstandard:
        res = 'nonstandard'
    elif addr_obj.type == blocksci.address_type.nulldata:
        res = 'nulldata'
    else:
        res = [addr_obj.address_string]
    return(res)


chain = blocksci.Blockchain("/var/data/blocksci_data/")

block_df = pd.DataFrame(columns=['height', 'block_hash',
                                 'no_transactions', 'timestamp'])

for height, block in enumerate(chain):

    txo = zip(block.txes.outputs.address,
              block.txes.outputs.value,
              block.txes.outputs.address_type)
    txo = [[{'address': addr_str(a), 'value': v, 'type': repr(t)} for a, v, t
            in zip(addr, val, tx_type)] for addr, val, tx_type in txo]

    txi = zip(block.txes.inputs.address,
              block.txes.inputs.value,
              block.txes.inputs.address_type)
    txi = [[{'address': addr_str(a), 'value': v, 'type': repr(t)} for a, v, t
            in zip(addr, val, tx_type)] for addr, val, tx_type in txi]

    # graphsense_transformed.transaction table
    block_tx_df = pd.DataFrame({'tx_hash': block.txes.hash,  # uint256
                                'coinbase': block.txes.is_coinbase,
                                'height': block.height,
                                'inputs': txi,
                                'outputs': txo,
                                'timestamp': block.timestamp,
                                'total_input': block.txes.input_value,
                                'total_output': block.txes.output_value
                                })
    # graphsense_transformed.block table
    block_df.loc[height] = (block.height, bytearray.fromhex(block.hash),
                            len(block.txes), block.timestamp)

    print(height, end='\r')
