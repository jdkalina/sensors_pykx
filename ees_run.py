import pykx as kx

db=kx.DB(path='/data/kdb')
def get_runs(tool_ids):
    """
    tool_ids: accepts either a single pykx.IntAtom type or a list of kx.IntAtom
    start_time: accepts a pykx.DateAtom type
    end_time: accepts a pykx.DateAtom type
    """
    #columns = ['tool_id','run_id','run_type','start_time','end_time','parent_run_id','parent_start_time']
    r= db.ees_run.select(
        where = kx.Column('tool_id').isin(tool_ids)
        )
    r = kx.q.xkey('tool_id', r)
    t= db.ees_tool_lookup.select(columns=['tool_id','tool_name'], 
                                 where = kx.Column('tool_id').isin(tool_ids))
    t = kx.q.xkey('tool_id', t)
    return kx.q.lj(r,t)

if __name__ == '__main__':
    tool_id = kx.toq(5001, ktype=kx.IntAtom)
    get_runs(tool_ids = data.tool_id,start_time=data.start_time,end_time=data.end_time)

