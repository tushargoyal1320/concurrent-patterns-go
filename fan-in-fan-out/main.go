package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sync"
)

func main() {

	ch1, err := read("file1.csv")
	if err != nil {
		fmt.Println("Error reading file1:", err)
		return
	}

	ch2, err := read("file2.csv")
	if err != nil {
		fmt.Println("Error reading file2:", err)
		return
	}

	exit := make(chan struct{})

	chM := merge2(ch1, ch2)

	go func() {
		for v := range chM {
			fmt.Println(v)
		}
		close(exit)
	}()

	<-exit
	fmt.Println("Finished processing all records.")

}

func merge1(chns ...<-chan []string) <-chan []string {

	var wg sync.WaitGroup

	out := make(chan []string)

	send := func(ch <-chan []string) {

		for v := range ch {
			out <- v
		}

		wg.Done()
	}

	wg.Add(len(chns))

	for _, ch := range chns {
		go send(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func merge2(chns ...<-chan []string) <-chan []string {

	len := len(chns)
	waitGroup := make(chan struct{}, len)

	out := make(chan []string)

	send := func(ch <-chan []string) {
		for v := range ch {
			out <- v
		}
		func() {
			waitGroup <- struct{}{}
		}()
	}

	for _, ch := range chns {
		go send(ch)
	}

	go func() {
		for range waitGroup {
			len--
			if len == 0 {
				break
			}
		}
		close(out)
	}()

	return out
}

func read(filePath string) (<-chan []string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil, err
	}

	reader := csv.NewReader(file)

	out := make(chan []string)

	go func() {
		for {
			record, err := reader.Read()

			if err == io.EOF {
				close(out)
				return
			}

			out <- record
		}

	}()

	return out, nil
}
