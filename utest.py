import unittest
import mock


import os

def rm(filename):
    os.remove(filename)
    
class A:
    def b(self):
        print "hello"
        
    def c(self):
        self.b()

class RmTestCase(unittest.TestCase):
    #@mock.patch("os.remove")
    def test_rm(self):
        with mock.patch("utest.A.b") as m:
            A().c()
            m.assert_called_with()
        
if __name__=='__main__':
    unittest.main()
    #suite = unittest.TestSuite()
    #suite.addTest(TestJob('job'))
    #uite.addTest(TestTask('task'))
    