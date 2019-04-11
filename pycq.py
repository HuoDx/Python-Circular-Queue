'''
@author: Daixuan Queue Huo
@date: Apr. 11th 2019
@version: 1.0 indev.
@license: GNU GPLv3 license
'''
from avltree import AVLTree
import threading

class CircularQueue:
    '''
    CircularQueue implements a running circular queue.\n\n

    This is mostly used to deal for timeout opertations, 
    especially when the data scale is large.\n\n
    
    Attributes
    ----------
    queue_length: int
        the element number of this circular queue
    update_time: int
        the update time (in seconds) of the pointer
    stoped: bool
        a boolean variable which marks whether this queue has stoped running
    pointer: int
        a pointer points a position in the circular queue
    circular_queue: list
        the main circular queue with length queue_length,
        with an AVL tree in each node.
    
    Methods
    -------
    stop()
        Stops the queue
    has_stoped()
        Checks whether the queue has stoped
    start()
        Starts running the circular queue
    poll(key):
        Extracts an element from the tree by the key given
    insert(key,value):
        insert a key-value pair that times out after seconds
    
    '''
    # the element number of this circular queue
    queue_length = None
    # the time (in seconds) which the pointer waits to move to the next position
    update_time = None
    # marks whether this queue has stoped
    stoped = None
    # is the main circular queue with length queue_length, with an AVL tree in each node.
    circular_queue = None
    # the pointer
    pointer = None
    # initializes all the variables in the class
    def initialize(self,queue_length,update_time):
        '''
        **THIS IS A PRIVATE METHOD.**\n
        You should NOT USE IT in most conditions.
        '''
        self.queue_length = queue_length
        self.update_time = update_time
        self.pointer = 0
        self.loop_counter = 0
        # it is not running now
        self.stoped = True
        # create a list with an AVL tree in each node
        self.circular_queue = [AVLTree(),]*queue_length

    # stops the queue
    def stop(self):
        '''
        Stops the queue
        '''
        self.stoped = True
    # check whether the queue has stoped
    def has_stoped(self):
        '''
        Examine whether the queue has stoped running
        '''
        return self.stoped
    # constructs the circular queue class, waiting to run
    def __init__(self,queue_length,update_time):
        '''
        the total timeout should be:
            timeout = queue_length * update_time
        where update_time is basically pricision of timeout in this circular queue
        Parameters
        ----------
        queue_length: int, required
            the length of this circular queue
        update_time: int, required
            the update time (in seconds)
        '''
    
        # initialize all the variables
        self.initialize(queue_length,update_time)

    # starts the circular queue
    def start(self):
        '''
        Starts running the circular queue
        '''

        # now it is not stoped. Woo-hoo!
        self.stoped = True
        #run the queue in a new thread
        try:
            first_run = threading.Thread(target=self.run)
            first_run.start()
            first_run.join()
        except Exception as e:
            print(e)
            #return to show method fails
            return {
                    'result': False,
                    'error_message': e
                   }
        # return to show method succeed
        return {'result': True}

    
    # updates the queue periodically
    def run(self):
        '''
        **THIS IS A PRIVATE METHOD.**\n
        You should NOT USE IT in most conditions
        '''
        if self.stoped:
            return None
        try:
            # schedules the next update
            threading.Timer(self.update_time,self.run)
            # deletes all the time out elements
            del(self.circular_queue[self.pointer])
            self.circular_queue.insert(AVLTree())
            # moves the pointer forward
            pointer = (pointer + 1)%self.queue_length
        except Exception as e:
            print(e)
            pass

    # extracts an element from the tree by a key given
    def poll(self,key):
        '''
        Extracts an element from the tree by a key given.
        Parameters
        ----------
        key: int, required
            the key given
        Returns
        -------
        the corresponding value to the key.\n
        if the key does not exist, None is returned
        '''
        value = None
        for element in self.circular_queue:
            try:
                node = element.get_node(key)
                # if the key is not here
                if node == None:
                    # find next element in the circular queue
                    continue
                value = node.value
                element.delete(key)
            except Exception as e:
                print(e)
                return None
        return value

    #Inserts a key-value pair
    def insert(self,key,value):
        '''
        Inserts a key-value pair
        Parameters
        ----------
        key: int, required
            the key
        value int,required
            the value
        '''
        self.circular_queue[(self.pointer - 1)%self.queue_length].insert(key,value)

    

        



        


