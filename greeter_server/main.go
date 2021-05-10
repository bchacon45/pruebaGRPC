package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"strconv"
	"database/sql"

	"google.golang.org/grpc"
	pb "https://github.com/bchacon45/pruebaGRPC/tree/main/helloworld"
    	_ "github.com/go-sql-driver/mysql"
)

type reporteJSON struct {
	Carnet          int `json: "carnet"`
	Nombre      string `json: "nombre"`
	Curso           string    `json: "curso"`
	Cuerpo_reporte string `json: "cuerpo_reporte"`
	Servidor_procesado string `json: "servidor_procesado"`
}

type asistenciaJSON struct {
	Carnet          int `json: "Carnet"`
	NombreEstudiante      string `json: "NombreEstudiante"`
	IdEvento           int    `json: "IdEvento"`
	NombreEvento string `json: "NombreEvento"`
	UrlCaptura   string `json: "UrlCaptura"`
	Captura 	 string `json: "Captura"`
	Servidor_procesado string `json: "Servidor_procesado"`
}

const (
	port = ":5002"
)

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedGreeterServer
}

// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	//log.Printf("Received1: %v", in.GetName())

	// Conexion a mongodb
	data := in.GetName()
	info := reporteJSON{}
	json.Unmarshal([]byte(data), &info)

	db, err := sql.Open("mysql", "admin:administrador@tcp(database-redes2-g14.cqzrquobie6y.us-east-2.rds.amazonaws.com:3306)/redes")

	if err != nil {
        panic(err.Error())
    }
    
    	defer db.Close()

	sentenciaPreparada, err := db.Prepare("INSERT INTO REPORTE (Carnet, Nombre, Curso_proyeto, Cuerpo, Servidor_procesado) VALUES(?, ?, ?, ?, ?)")
	if err != nil {
		//return err
	}
	defer sentenciaPreparada.Close()
	
	_, err = sentenciaPreparada.Exec(strconv.FormatInt(int64(info.Carnet), 10), info.Nombre, info.Curso, info.Cuerpo_reporte, info.Servidor_procesado)
	if err != nil {
		//return err
	}
	//return nil
	
	log.Printf("Carnet: %v, Nombre: %v, Curso: %v, Cuerpo de reporte: %v, Servidor procesado: %v", strconv.FormatInt(int64(info.Carnet), 10),info.Nombre,info.Curso,info.Cuerpo_reporte,info.Servidor_procesado)

	// Respuesta al cliente grpc
	return &pb.HelloReply{Message: "Servidor recibio la informacion correctamente."}, nil
}


// Insertar data a asistencia
func (s *server) RegistrarAsistencia(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	//log.Printf("Received1: %v", in.GetName())

	// Conexion a mongodb
	data := in.GetName()
	info := asistenciaJSON{}
	json.Unmarshal([]byte(data), &info)

	db, err := sql.Open("mysql", "admin:administrador@tcp(database-redes2-g14.cqzrquobie6y.us-east-2.rds.amazonaws.com:3306)/redes")

	if err != nil {
        panic(err.Error())
    }
    
    	defer db.Close()

	sentenciaPreparada, err := db.Prepare("INSERT INTO datos (Carnet, NombreEstudiante, IdEvento, NombreEvento, UrlCaptura, Captura,Servidor_procesado) VALUES(?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		//return err
	}
	defer sentenciaPreparada.Close()
	
	_, err = sentenciaPreparada.Exec(strconv.FormatInt(int64(info.Carnet), 10), info.NombreEstudiante, strconv.FormatInt(int64(info.IdEvento), 10), info.NombreEvento, info.UrlCaptura,info.Captura,info.Servidor_procesado)
	if err != nil {
		//return err
	}
	//return nil
	
	log.Printf("Carnet: %v, Nombre: %v, IdEvento: %v, Nombre Evento: %v, Servidor procesado: %v", strconv.FormatInt(int64(info.Carnet), 10),info.NombreEstudiante,strconv.FormatInt(int64(info.IdEvento), 10),info.NombreEvento,info.Servidor_procesado)

	// Respuesta al cliente grpc
	return &pb.HelloReply{Message: "Servidor recibio la informacion correctamente."}, nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		//log.Printf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		//log.Printf("failed to serve: %v", err)
	}
}
