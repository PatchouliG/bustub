# overview

project 2, b tree concurrent index 
add b tree lock manager to handle page lock/unlock
insert, getValue, Remove method need lock manager
 
 <!-- todo  -->
 iterator how to lock

# lock manager

## interface

construct(btree,mode) // need btree to update root, need transaction to update w/r set ,mode is read/insert/remove
addChildForInsert( child node)
addChildForDelete( child node)
addChildForRead( child node)
popChild() pop last child,unlock
currentNode() // return last child

transactionHelp
wrap add child, add page to transation 

## inplement

require lock, update transaction,unlock parent if ok
unlock and unpin all page in destruction 

## notice

### hint in cmu page
unlock before unpin

## test

test lockManager;
