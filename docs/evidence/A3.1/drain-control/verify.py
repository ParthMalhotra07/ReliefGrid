import importlib.util,json,pathlib,sys
spec=importlib.util.spec_from_file_location('app_demo',pathlib.Path('scripts/demos/app_demo.py'))
app=importlib.util.module_from_spec(spec);spec.loader.exec_module(app)
demo=app.Demo(2,pathlib.Path(sys.argv[1]))
original=demo.drain_outbox

def pages():
    return [demo.coordinator.call('GET',f'/internal/outbox?shard={s}&limit=100&consistency=strong',{200})[0] for s in range(3)]

def seeded_drain():
    # This controlled helper regression is separate from the 500/50 fixture.
    initial=pages()
    assert all(not p['events'] for p in initial),initial
    resource=demo.resource('pending-handoff-control',kind='WATER')
    before=pages()
    pending=[e for p in before for e in p['events']]
    assert len(pending)==1,(resource,pending)
    demo.save('pending-before',before)
    original()
    after=pages()
    demo.save('pending-after',after)
    assert all(not p['events'] for p in after),after
    report=json.loads((demo.out/'handoff-drain.log').read_text())
    assert report['Read']==report['Published']==report['Marked']==1,report
    demo.check(True,'one committed event published with a routed confirm and marked after all admission workers stopped')

demo.drain_outbox=seeded_drain
demo.audit_without_admission()
demo.save('result',{'result':'PASS','checks':demo.checks})
