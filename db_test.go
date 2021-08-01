package gokv_test

import (
	"fmt"
	"gokv"
	"testing"
)

func TestOpen(t *testing.T) {
	db, err := gokv.Open("test")
	if err != nil {
		t.Fatal(err)
	} else if db == nil {
		t.Fatal("expected db")
	}
}

func TestPut(t *testing.T) {
	db, err := gokv.Open("test")
	if err != nil {
		t.Fatal(err)
	} else if db == nil {
		t.Fatal("expected db")
	}
	err = db.Put("test", []byte("123"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestGet(t *testing.T) {
	db, err := gokv.Open("test")
	if err != nil {
		t.Fatal(err)
	} else if db == nil {
		t.Fatal("expected db")
	}
	db.Put("test", []byte("123"))
	_, err = db.Get("test")
	if err != nil {
		t.Fatal(err)
	}
}

func TestDel(t *testing.T) {
	db, err := gokv.Open("test")
	if err != nil {
		t.Fatal(err)
	} else if db == nil {
		t.Fatal("expected db")
	}
	db.Put("test", []byte("123"))
	err = db.Del("test")
	if err != nil {
		t.Fatal(err)
	}
}

func TestMaxSize(t *testing.T) {
	db, err := gokv.Open("test")
	if err != nil {
		t.Fatal(err)
	} else if db == nil {
		t.Fatal("expected db")
	}
	for i := 0; i < 2048; i++ {
		err = db.Put("test"+fmt.Sprint(i), []byte("123"))
		if err != nil {
			t.Fatal(err)
		}

	}
}

func TestLock(t *testing.T) {
	db, err := gokv.Open("test")
	if err != nil {
		t.Fatal(err)
	} else if db == nil {
		t.Fatal("expected db")
	}

	times := 2048 * 128
	ch := make(chan struct{})
	for i := 0; i < times; i++ {
		go func(i int) {
			db.Get("test" + fmt.Sprint(i%1024))
			// fmt.Println(string(val))
			ch <- struct{}{}
		}(i)
	}
	for i := 0; i < times; i++ {
		<-ch
	}
}

func TestClose(t *testing.T) {
	db, err := gokv.Open("test")
	if err != nil {
		t.Fatal(err)
	} else if db == nil {
		t.Fatal("expected db")
	}
	for i := 0; i < 2048; i++ {
		err = db.Put("test"+fmt.Sprint(i), []byte(fmt.Sprint(i)))
		if err != nil {
			t.Fatal(err)
		}

	}
	if err = db.Close(); err != nil {
		t.Fatal("close db fail")
	}
}
