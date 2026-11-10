import copy
import unittest
from metrics_fault import judge, mutation_version

class FaultEvidenceTest(unittest.TestCase):
 def test_mutation_envelope_must_match_the_proposed_operation(self):
  body=dict(ringVersion=1,result=dict(outcome='OK',operationId='op1',key='probe',version=1))
  self.assertEqual(mutation_version(body,'op1','probe',1),1)
  for field,value in [('outcome','VERSION_CONFLICT'),('operationId','other'),('key','foreign'),('version',None)]:
   invalid=copy.deepcopy(body);invalid['result'][field]=value
   with self.subTest(field=field),self.assertRaises(AssertionError):mutation_version(invalid,'op1','probe',1)
 def samples(self):
  return [dict(phase='baseline',leader='old',oldUp=1,oldDown=0,lag=0,age=10,targets=9),
          dict(phase='fault',leader='new',oldUp=0,oldDown=1,lag=7,age=15,targets=8),
          dict(phase='healed',leader='new',oldUp=1,oldDown=0,lag=0,age=30,targets=9,caughtUp=True)]
 def test_actual_fault_and_recovery(self):
  self.assertEqual(judge(self.samples())['peakLag'],7)
 def test_no_samples_or_no_fault_cannot_pass(self):
  for rows in [[],self.samples()[:1],[self.samples()[0],self.samples()[-1]]]:
   with self.subTest(rows=rows),self.assertRaises(AssertionError):judge(rows)
 def test_missing_flat_lag_and_static_age_cannot_pass(self):
  for key,value in [('lag',None),('lag',0),('age',10),('leader','old')]:
   rows=copy.deepcopy(self.samples());rows[1][key]=value
   if key=='age':rows[2]['age']=10
   with self.subTest(key=key,value=value),self.assertRaises(AssertionError):judge(rows)
 def test_barrier_index_gaps_are_not_unapplied_commands(self):
  rows=self.samples();rows[0]['lag']=46;rows[1]['lag']=2;rows[2]['lag']=642;rows[2]['caughtUp']=True
  self.assertEqual(judge(rows)['peakLag'],642)
 def test_zero_gap_is_not_a_substitute_for_authoritative_catchup(self):
  rows=self.samples();rows[-1]['caughtUp']=False
  with self.assertRaises(AssertionError):judge(rows)
 def test_incomplete_recovery_cannot_pass(self):
  for key,value in [('targets',8),('oldUp',0),('lag',None),('caughtUp',False)]:
   rows=copy.deepcopy(self.samples());rows[-1][key]=value
   with self.subTest(key=key),self.assertRaises(AssertionError):judge(rows)

if __name__=='__main__':unittest.main()
