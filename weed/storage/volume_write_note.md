# PhantomRead Demo

## writer

### Expected:

case 1:
-   write Needle
-   read same Needle

case 2:
-   write Needle failed

### Wrong:
-   write Needle successfully
-   cannot read

## cleaner

### Expected:

case 1:
-   not empty error

case 2:
-   delete empty

### Wrong:
-   delete non empty (have write between isEmpty and Destroy)
