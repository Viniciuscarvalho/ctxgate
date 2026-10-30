package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	if path := os.Getenv("CTXGATE_FAKE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(fmt.Sprint(os.Getpid())), 0o600); err != nil {
			panic(err)
		}
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}
	select {}
}
