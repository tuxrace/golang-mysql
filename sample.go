package sample

import (
	"fmt"
	"io/ioutil"
)

func main() {
	d1 := "yoyo this"
	ioutil.WriteFile("test.json", d1, 0644)
	fmt.Println(d1)
}
