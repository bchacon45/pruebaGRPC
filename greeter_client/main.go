/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package main implements a client for Greeter service.
package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
)

const (
	address     = "18.191.181.209:5002"
)

func Inicio(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hola GRPC")
}

func createNewArticle(w http.ResponseWriter, r *http.Request) {
	// get the body of our POST request
	// return the string response containing the request body
	reqBody, _ := ioutil.ReadAll(r.Body)
	//fmt.Fprintf(w, "%+v...Holis :)", string(reqBody))

	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := c.SayHello(ctx, &pb.HelloRequest{Name: string(reqBody)})

	log.Printf("Greeting: %s", res.GetMessage())
	fmt.Fprintf(w, res.GetMessage())
	
}

func CORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		// Set headers
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "*")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		fmt.Println("ok")

		// Next
		next.ServeHTTP(w, r)
		return
	})
}

func handleRequests() {
	myRouter := mux.NewRouter().StrictSlash(true)
	myRouter.use(CORS)
	myRouter.HandleFunc("/", Inicio).Methods("GET")
	myRouter.HandleFunc("/", createNewArticle).Methods("POST")
	
	http.ListenAndServe(":5001", myRouter)
}

func main() {
	handleRequests()
	// Set up a connection to the server.
}
