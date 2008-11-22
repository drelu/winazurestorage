from winazurestorage import *

def do_blob_tests():
    '''Expected output:
        Starting blob tests
                create_container: 201
                put_blob: 201
                get_blob: Hello, World!
                delete_container: 202
        Done.
    '''
    print "Starting blob tests"
    blobs = BlobStorage()
    print "\tcreate_container: %d" % blobs.create_container("testcontainer", True)
    print "\tput_blob: %d" % blobs.put_blob("testcontainer", "testblob.txt", "Hello, World!", "text/plain")
    print "\tget_blob: %s" % blobs.get_blob("testcontainer", "testblob.txt")
    print "\tdelete_container: %d" % blobs.delete_container("testcontainer")
    print "Done."

def run_tests():
    do_blob_tests()

if __name__ == '__main__':
    run_tests()