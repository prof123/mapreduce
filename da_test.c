#include "da.h"
#include "dyn_array.h"
#include <stdio.h>

int main() {
	DYNARRAY a = new_array(sizeof(ELEMENT));
	int c = insert_into(a,32);
	printf("%d\n",a->DATA[0]);
}
