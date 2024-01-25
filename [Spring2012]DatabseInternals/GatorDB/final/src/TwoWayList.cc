#ifndef _TWO_WAY_LIST_C
#define _TWO_WAY_LIST_C

#include "TwoWayList.h"

#include <stdlib.h>
#include <iostream>

using namespace std;


// create an alias of the given TwoWayList
template <class Type>
TwoWayList <Type> :: TwoWayList (TwoWayList &me) {

	/* <Akshay> - Copy Constructor: Creates a new header and assigns
		And assigns the properties using the values of the reference
		passed
	*/
	list = new (std::nothrow) Header;
	if (list == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}

	list->first = me.list->first;
	list->last = me.list->last;
	list->current = me.list->current;
	list->leftSize = me.list->leftSize;
	list->rightSize = me.list->rightSize;
}

// basic constructor function
template <class Type>
TwoWayList <Type> :: TwoWayList ()
{
	/* <Akshay> - Constructor: Creates a new header and 2 new nodes
		and assigns the first node as the header first and current and the other node as
		last pointer

	*/
	// allocate space for the header
	list = new (std::nothrow) Header;
	if (list == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}
	
	// set up the initial values for an empty list
	list->first = new (std::nothrow) Node;
	if (list->first == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}

	list->last = new (std::nothrow) Node;
	if (list->last == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}

	list->current = list->first;
	list->leftSize = 0;
	/* <Akshay> - Right Size = 0. When the current pointer is pointing to
		previous of the last node, then list has reached the finish */
	list->rightSize = 0;
	list->first->next = list->last;
	list->last->previous = list->first;

}

// basic deconstructor function
template <class Type>
TwoWayList <Type> :: ~TwoWayList ()
{

	// remove everything
	MoveToStart ();
	while (RightLength ()>0) {
		/* Removes (frees) all data first */
		Type temp;
		Remove (&temp);
	}

	// kill all the nodes
	for (int i = 0; i <= list->leftSize + list->rightSize; i++) {
		/* <Akshay> - Updates the first node and then removes the previous 
			first node
		*/
		list->first = list->first->next;
		delete list->first->previous;
	} 
	delete list->first;
	
	// kill the header
	delete list;

}

// swap operator
template <class Type> void
TwoWayList <Type> :: operator &= (TwoWayList & List)
{

	Header *temp = List.list;
	List.list = list;
	list = temp;
	
}

// make the first node the current node
template <class Type> void
TwoWayList <Type> :: MoveToStart ()
{
	
	list->current = list->first;
	list->rightSize += list->leftSize;
	list->leftSize = 0;

}

// make the first node the current node /* <Akshay> - first node ??? */
template <class Type> void
TwoWayList <Type> :: MoveToFinish ()
{
	
	list->current = list->last->previous;
	list->leftSize += list->rightSize;
	list->rightSize = 0;

}

// determine the number of items to the left of the current node
template <class Type> int 
TwoWayList <Type> :: LeftLength ()
{
	return (list->leftSize);
}

// determine the number of items to the right of the current node
template <class Type> int 
TwoWayList <Type> :: RightLength ()
{
   //  cout<<"Right Size = "<<list->rightSize<<endl;
	return (list->rightSize);
}

// swap the right sides of two lists
template <class Type> void 
TwoWayList <Type> :: SwapRights (TwoWayList & List)
{
	/* <Akshay> - Swaps the nodes, end points i,e last node and the right size */
	
	// swap out everything after the current nodes
	Node *left_1 = list->current;
	Node *right_1 = list->current->next;
	Node *left_2 = List.list->current;
	Node *right_2 = List.list->current->next;

	left_1->next = right_2;
	right_2->previous = left_1;
	left_2->next = right_1;
	right_1->previous = left_2;
	
	// set the new endpoints
	Node *temp = list->last;
	list->last = List.list->last;	
	List.list->last = temp;	

	int tempint = List.list->rightSize;
	List.list->rightSize = list->rightSize;
	list->rightSize = tempint;

}

// swap the leftt sides of the two lists
template <class Type> void 
TwoWayList <Type> :: SwapLefts (TwoWayList & List)
{
	/* <Akshay> - Swaps the nodes, start points i,e first node and the left size */
	// swap out everything after the current nodes
	Node *left_1 = list->current;
	Node *right_1 = list->current->next;
	Node *left_2 = List.list->current;
	Node *right_2 = List.list->current->next;

	left_1->next = right_2;
	right_2->previous = left_1;
	left_2->next = right_1;
	right_1->previous = left_2;

	// set the new frontpoints
	Node *temp = list->first;
	list->first = List.list->first;	
	List.list->first = temp;	

	// set the new current nodes
	temp = list->current;
	list->current = List.list->current;	
	List.list->current = temp;	

	int tempint = List.list->leftSize;
	List.list->leftSize = list->leftSize;
	list->leftSize = tempint;
}

// move forwards through the list 
template <class Type> void 
TwoWayList <Type> :: Advance ()
{

	(list->rightSize)--;
	(list->leftSize)++;
	list->current = list->current->next;

}

// move backwards through the list
template <class Type> void 
TwoWayList <Type> :: Retreat ()
{

	(list->rightSize)++;
	(list->leftSize)--;
	list->current = list->current->previous;

}

// insert an item at the current poition
template <class Type> void
TwoWayList <Type> :: Insert (Type *Item)
{

	Node *temp = new (std::nothrow) Node;
	if (temp == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}

	Node *left = list->current;
	Node *right = list->current->next;

	left->next = temp;
	temp->previous = left;
	temp->next = right;
	temp->data = new (std::nothrow) Type;
	if (temp->data == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}

	right->previous = temp;

	temp->data->Consume (Item);

	/* <Akshay> - Right size elements to the right, including itself */
	list->rightSize += 1;

}

/* <Akshay> - gets the data of the node which is offset 
	nodes to the right of the current node
*/
// get a reference to the currentitemin the list
// does not change the current pointer
template <class Type> Type* 
TwoWayList <Type> ::  Current (int offset)
{
	Node *temp = list->current->next;
	for (int i = 0; i < offset; i++) {
		temp = temp->next;
	}
        // cout<<"data : "<<temp->data<<endl;
	return temp->data;
}

template <class Type> void
TwoWayList <Type> ::  GetCurrent (Type* item)
{
    Node *temp = list->current->next;
    if(NULL != temp)
    {

        // Type* data = temp->data;
        
        item->Copy(temp->data);
    }
}

// remove an item from the current poition
template <class Type> void
TwoWayList <Type> :: Remove (Type *Item)
{
        
	Node *temp = list->current->next;
	list->current->next = temp->next;
	temp->next->previous = list->current;
        
	/******************* Not Generic */
	Item->Consume(temp->data);

	delete temp;

	(list->rightSize)--;
}



#endif
	
