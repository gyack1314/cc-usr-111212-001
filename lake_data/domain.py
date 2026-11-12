"""核对文件身份和schema编号，不计算可见行。"""
from .io import asset,integer

def summarize(v,root):
    specs={s['id'] for s in v['partition_specs']};schemas={s['id'] for s in v['schemas']}
    for f in v['files']:
        if f['spec_id'] not in specs or f['schema_id'] not in schemas:raise ValueError('文件schema或spec未知')
        integer(f['data_sequence']);integer(f['file_sequence']);asset(root,f['asset'])
    return {'files':len(v['files']),'schemas':len(schemas),'partition_specs':len(specs),'snapshot':v['snapshot']['id']}
