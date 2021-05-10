package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"fmt"
	"strconv"
	"database/sql"

	"google.golang.org/grpc"
	pb "github.com/bchacon45/pruebaGRPC/helloworld"
    	_ "github.com/go-sql-driver/mysql"
)

type reporteJSON struct {
	Valor          int `json: "valor" ?`
	Carnet          int `json: "carnet" ?`
	Nombre      string `json: "nombre" ?`
	Curso           string    `json: "curso" ?`
	Cuerpo_reporte string `json: "cuerpo_reporte" ?`
	Servidor_procesado string `json: "servidor_procesado" ? `
	IdEvento           int    `json: "IdEvento" ?`
	NombreEvento string `json: "NombreEvento" ?`
	UrlCaptura   string `json: "UrlCaptura" ?`
	Captura 	 string `json: "Captura" ?`
}

type reporte struct {
	ReporteId	int
	Carnet          string
	Nombre      string
	Curso           string
	Cuerpo_reporte string
	Servidor_procesado string
	Fecha	string
} 

type asistencia struct {
	AsistenciaId	int
	Carnet          string
	NombreEstudiante      string
	IdEvento           string
	NombreEvento string
	UrlCaptura string
	Captura	string
	Servidor_procesado	string
	FechaHora	string
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

	prueba := info.Valor

	switch prueba {
	case 1:
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

	case 2:
		db, err := sql.Open("mysql", "admin:administrador@tcp(database-redes2-g14.cqzrquobie6y.us-east-2.rds.amazonaws.com:3306)/redes")

		if err != nil {
       		 panic(err.Error())
    	}
    
    	defer db.Close()

		sentenciaPreparada, err := db.Prepare("insert into datos ( Carnet,    NombreEstudiante,    IdEvento ,    NombreEvento ,    UrlCaptura ,    Captura ,    Servidor_procesado ) VALUES(?, ?, ?, ?, ?, ?, ?)")
		if err != nil {
		//return err
		}
		defer sentenciaPreparada.Close()
	
		_, err = sentenciaPreparada.Exec(strconv.FormatInt(int64(info.Carnet), 10), info.Nombre, strconv.FormatInt(int64(info.IdEvento), 10), info.NombreEvento, info.UrlCaptura,info.Captura,info.Servidor_procesado)
		if err != nil {
		//return err
		}
		//return nil
	
		log.Printf("Carnet: %v, Nombre: %v, IdEvento: %v, Nombre Evento: %v, Servidor procesado: %v", strconv.FormatInt(int64(info.Carnet), 10),info.Nombre,strconv.FormatInt(int64(info.IdEvento), 10),info.NombreEvento,info.Servidor_procesado)

		// Respuesta al cliente grpc
		return &pb.HelloReply{Message: "Servidor recibio la informacion correctamente."}, nil
	
	case 3:

	db, err := sql.Open("mysql", "admin:administrador@tcp(database-redes2-g14.cqzrquobie6y.us-east-2.rds.amazonaws.com:3306)/redes")

		if err != nil {
       		 panic(err.Error())
    	}
    
    	defer db.Close()
	
	reportes := ""
	
	filas, err := db.Query("SELECT ReporteId, Carnet,    Nombre, Curso_proyeto,     Cuerpo,   Servidor_procesado, Fecha FROM REPORTE")

	if err != nil {
		return nil, err
	}

	// Si llegamos aquí, significa que no ocurrió ningún error
	defer filas.Close()

	// Aquí vamos a "mapear" lo que traiga la consulta en el while de más abajo
	var c reporte

	// Recorrer todas las filas, en un "while"
	for filas.Next() {
		err = filas.Scan(&c.ReporteId, &c.Carnet, &c.Nombre, &c.Curso, &c.Cuerpo_reporte, &c.Servidor_procesado, &c.Fecha)
		// Al escanear puede haber un error
		if err != nil {
			return nil, err
		}
		// Y si no, entonces agregamos lo leído al arreglo
		if(reportes == ""){
			reportes = "{ \"ReporteId\": " + fmt.Sprint(c.ReporteId) +", \"carnet\": \"" + c.Carnet + "\", \"nombre\": \"" + c.Nombre + "\", \"cuerpo_reporte\": \"" + c.Cuerpo_reporte + "\", \"curso\": \"" + c.Curso + "\", \"Fecha\": \"" + c.Fecha + "\", \"servidor_procesado\": \"" + c.Servidor_procesado + "\"}"
		}else{
			reportes += ",\n{ \"ReporteId\": " + fmt.Sprint(c.ReporteId) +", \"carnet\": \"" + c.Carnet + "\", \"nombre\": \"" + c.Nombre + "\", \"cuerpo_reporte\": \"" + c.Cuerpo_reporte + "\", \"curso\": \"" + c.Curso + "\", \"Fecha\": \"" + c.Fecha + "\", \"servidor_procesado\": \"" + c.Servidor_procesado + "\" }"
		}
		
	}
	reportes = "[\n" + reportes + "\n]"

	// Vacío o no, regresamos el arreglo de contactos
	return &pb.HelloReply{Message: reportes}, nil
	
	
	case 4:

	db, err := sql.Open("mysql", "admin:administrador@tcp(database-redes2-g14.cqzrquobie6y.us-east-2.rds.amazonaws.com:3306)/redes")

		if err != nil {
       		 panic(err.Error())
    	}
    
    	defer db.Close()
	
	reportes := ""
	
	filas, err := db.Query("SELECT ReporteId, Carnet,    Nombre, Curso_proyeto,     Cuerpo,   Servidor_procesado, Fecha FROM REPORTE WHERE Carnet = ?", info.Carnet)

	if err != nil {
		return nil, err
	}

	// Si llegamos aquí, significa que no ocurrió ningún error
	defer filas.Close()

	// Aquí vamos a "mapear" lo que traiga la consulta en el while de más abajo
	var c reporte

	// Recorrer todas las filas, en un "while"
	for filas.Next() {
		err = filas.Scan(&c.ReporteId, &c.Carnet, &c.Nombre, &c.Curso, &c.Cuerpo_reporte, &c.Servidor_procesado, &c.Fecha)
		// Al escanear puede haber un error
		if err != nil {
			return nil, err
		}
		// Y si no, entonces agregamos lo leído al arreglo
		if(reportes == ""){
			reportes = "{ \"ReporteId\": " + fmt.Sprint(c.ReporteId) +", \"carnet\": \"" + c.Carnet + "\", \"nombre\": \"" + c.Nombre + "\", \"cuerpo_reporte\": \"" + c.Cuerpo_reporte + "\", \"curso\": \"" + c.Curso + "\", \"Fecha\": \"" + c.Fecha + "\", \"servidor_procesado\": \"" + c.Servidor_procesado + "\" }"
		}else{
			reportes += ",\n{ \"ReporteId\": " + fmt.Sprint(c.ReporteId) +", \"carnet\": \"" + c.Carnet + "\", \"nombre\": \"" + c.Nombre + "\", \"cuerpo_reporte\": \"" + c.Cuerpo_reporte + "\", \"curso\": \"" + c.Curso + "\", \"Fecha\": \"" + c.Fecha + "\", \"servidor_procesado\": \"" + c.Servidor_procesado + "\"}"
		}
		
	}
	reportes = "[\n" + reportes + "\n]"

	// Vacío o no, regresamos el arreglo de contactos
	return &pb.HelloReply{Message: reportes}, nil
	
	case 5:

	db, err := sql.Open("mysql", "admin:administrador@tcp(database-redes2-g14.cqzrquobie6y.us-east-2.rds.amazonaws.com:3306)/redes")

		if err != nil {
       		 panic(err.Error())
    	}
    
    	defer db.Close()
	
	asistencias := ""
	
	filas, err := db.Query("SELECT AsistenciaId, Carnet,    NombreEstudiante, IdEvento,     NombreEvento,   UrlCaptura, Captura, Servidor_procesado, FechaHora FROM datos")

	if err != nil {
		return nil, err
	}

	// Si llegamos aquí, significa que no ocurrió ningún error
	defer filas.Close()

	// Aquí vamos a "mapear" lo que traiga la consulta en el while de más abajo
	var c asistencia

	// Recorrer todas las filas, en un "while"
	for filas.Next() {
		err = filas.Scan(&c.AsistenciaId, &c.Carnet, &c.NombreEstudiante, &c.IdEvento, &c.NombreEvento, &c.UrlCaptura, &c.Captura, &c.Servidor_procesado , &c.FechaHora)
		// Al escanear puede haber un error
		if err != nil {
			return nil, err
		}
		// Y si no, entonces agregamos lo leído al arreglo
		if(asistencias == ""){
			asistencias = "{ \"AsistenciaId\": " + fmt.Sprint(c.AsistenciaId) +", \"carnet\": \"" + c.Carnet + "\", \"nombre\": \"" + c.NombreEstudiante + "\", \"IdEvento\": \"" + c.IdEvento + "\", \"NombreEvento\": \"" + c.NombreEvento + "\", \"UrlCaptura\": \"" + c.UrlCaptura + "\", \"Captura\": \"" + c.Captura + "\", \"servidor_procesado\": \"" + c.Servidor_procesado + "\", \"FechaHora\": \"" + c.FechaHora + "\"}"
		}else{
			asistencias += ",\n \"AsistenciaId\": " + fmt.Sprint(c.AsistenciaId) +", \"carnet\": \"" + c.Carnet + "\", \"nombre\": \"" + c.NombreEstudiante + "\", \"IdEvento\": \"" + c.IdEvento + "\", \"NombreEvento\": \"" + c.NombreEvento + "\", \"UrlCaptura\": \"" + c.UrlCaptura + "\", \"Captura\": \"" + c.Captura + "\", \"servidor_procesado\": \"" + c.Servidor_procesado + "\", \"FechaHora\": \"" + c.FechaHora + "\"}"
		}
		
	}
	asistencias = "[\n" + asistencias + "\n]"

	// Vacío o no, regresamos el arreglo de contactos
	return &pb.HelloReply{Message: asistencias}, nil
	
	case 6:

	db, err := sql.Open("mysql", "admin:administrador@tcp(database-redes2-g14.cqzrquobie6y.us-east-2.rds.amazonaws.com:3306)/redes")

		if err != nil {
       		 panic(err.Error())
    	}
    
    	defer db.Close()
	
	asistencias := ""
	
	filas, err := db.Query("SELECT AsistenciaId, Carnet,    NombreEstudiante, IdEvento,     NombreEvento,   UrlCaptura, Captura, Servidor_procesado, FechaHora FROM datos WHERE Carnet = ?", info.Carnet)

	if err != nil {
		return nil, err
	}

	// Si llegamos aquí, significa que no ocurrió ningún error
	defer filas.Close()

	// Aquí vamos a "mapear" lo que traiga la consulta en el while de más abajo
	var c asistencia

	// Recorrer todas las filas, en un "while"
	for filas.Next() {
		err = filas.Scan(&c.AsistenciaId, &c.Carnet, &c.NombreEstudiante, &c.IdEvento, &c.NombreEvento, &c.UrlCaptura, &c.Captura, &c.Servidor_procesado , &c.FechaHora)
		// Al escanear puede haber un error
		if err != nil {
			return nil, err
		}
		// Y si no, entonces agregamos lo leído al arreglo
		if(asistencias == ""){
			asistencias = "{ \"AsistenciaId\": " + fmt.Sprint(c.AsistenciaId) +", \"carnet\": \"" + c.Carnet + "\", \"nombre\": \"" + c.NombreEstudiante + "\", \"IdEvento\": \"" + c.IdEvento + "\", \"NombreEvento\": \"" + c.NombreEvento + "\", \"UrlCaptura\": \"" + c.UrlCaptura + "\", \"Captura\": \"" + c.Captura + "\", \"servidor_procesado\": \"" + c.Servidor_procesado + "\", \"FechaHora\": \"" + c.FechaHora + "\"}"
		}else{
			asistencias += ",\n \"AsistenciaId\": " + fmt.Sprint(c.AsistenciaId) +", \"carnet\": \"" + c.Carnet + "\", \"nombre\": \"" + c.NombreEstudiante + "\", \"IdEvento\": \"" + c.IdEvento + "\", \"NombreEvento\": \"" + c.NombreEvento + "\", \"UrlCaptura\": \"" + c.UrlCaptura + "\", \"Captura\": \"" + c.Captura + "\", \"servidor_procesado\": \"" + c.Servidor_procesado + "\", \"FechaHora\": \"" + c.FechaHora + "\"}"
		}
		
	}
	asistencias = "[\n" + asistencias + "\n]"

	// Vacío o no, regresamos el arreglo de contactos
	return &pb.HelloReply{Message: asistencias}, nil
	
	case 7:

	db, err := sql.Open("mysql", "admin:administrador@tcp(database-redes2-g14.cqzrquobie6y.us-east-2.rds.amazonaws.com:3306)/redes")

		if err != nil {
       		 panic(err.Error())
    	}
    
    	defer db.Close()
	
	asistencias := ""
	
	filas, err := db.Query("SELECT AsistenciaId, Carnet,    NombreEstudiante, IdEvento,     NombreEvento,   UrlCaptura, Captura, Servidor_procesado, FechaHora FROM datos WHERE IdEvento = ?", info.IdEvento)

	if err != nil {
		return nil, err
	}

	// Si llegamos aquí, significa que no ocurrió ningún error
	defer filas.Close()

	// Aquí vamos a "mapear" lo que traiga la consulta en el while de más abajo
	var c asistencia

	// Recorrer todas las filas, en un "while"
	for filas.Next() {
		err = filas.Scan(&c.AsistenciaId, &c.Carnet, &c.NombreEstudiante, &c.IdEvento, &c.NombreEvento, &c.UrlCaptura, &c.Captura, &c.Servidor_procesado , &c.FechaHora)
		// Al escanear puede haber un error
		if err != nil {
			return nil, err
		}
		// Y si no, entonces agregamos lo leído al arreglo
		if(asistencias == ""){
			asistencias = "{ \"AsistenciaId\": " + fmt.Sprint(c.AsistenciaId) +", \"carnet\": \"" + c.Carnet + "\", \"nombre\": \"" + c.NombreEstudiante + "\", \"IdEvento\": \"" + c.IdEvento + "\", \"NombreEvento\": \"" + c.NombreEvento + "\", \"UrlCaptura\": \"" + c.UrlCaptura + "\", \"Captura\": \"" + c.Captura + "\", \"servidor_procesado\": \"" + c.Servidor_procesado + "\", \"FechaHora\": \"" + c.FechaHora + "\"}"
		}else{
			asistencias += ",\n \"AsistenciaId\": " + fmt.Sprint(c.AsistenciaId) +", \"carnet\": \"" + c.Carnet + "\", \"nombre\": \"" + c.NombreEstudiante + "\", \"IdEvento\": \"" + c.IdEvento + "\", \"NombreEvento\": \"" + c.NombreEvento + "\", \"UrlCaptura\": \"" + c.UrlCaptura + "\", \"Captura\": \"" + c.Captura + "\", \"servidor_procesado\": \"" + c.Servidor_procesado + "\", \"FechaHora\": \"" + c.FechaHora + "\"}"
		}
		
	}
	asistencias = "[\n" + asistencias + "\n]"

	// Vacío o no, regresamos el arreglo de contactos
	return &pb.HelloReply{Message: asistencias}, nil
	
	case 8:

	db, err := sql.Open("mysql", "admin:administrador@tcp(database-redes2-g14.cqzrquobie6y.us-east-2.rds.amazonaws.com:3306)/redes")

		if err != nil {
       		 panic(err.Error())
    	}
    
    	defer db.Close()
	
	reportes := ""
	
	filas, err := db.Query("SELECT ReporteId, Carnet,    Nombre, Curso_proyeto,     Cuerpo,   Servidor_procesado, Fecha FROM REPORTE where Carnet = ? order by Fecha desc limit 1", info.Carnet)

	if err != nil {
		return nil, err
	}

	// Si llegamos aquí, significa que no ocurrió ningún error
	defer filas.Close()

	// Aquí vamos a "mapear" lo que traiga la consulta en el while de más abajo
	var c reporte

	// Recorrer todas las filas, en un "while"
	for filas.Next() {
		err = filas.Scan(&c.ReporteId, &c.Carnet, &c.Nombre, &c.Curso, &c.Cuerpo_reporte, &c.Servidor_procesado, &c.Fecha)
		// Al escanear puede haber un error
		if err != nil {
			return nil, err
		}
		// Y si no, entonces agregamos lo leído al arreglo
			reportes = "{ \"ReporteId\": " + fmt.Sprint(c.ReporteId) +", \"carnet\": \"" + c.Carnet + "\", \"nombre\": \"" + c.Nombre + "\", \"cuerpo_reporte\": \"" + c.Cuerpo_reporte + "\", \"curso\": \"" + c.Curso + "\", \"Fecha\": \"" + c.Fecha + "\", \"servidor_procesado\": \"" + c.Servidor_procesado + "\"}"
		
	}
	reportes = "[\n" + reportes + "\n]"

	// Vacío o no, regresamos el arreglo de contactos
	return &pb.HelloReply{Message: reportes}, nil
	
	default:
		log.Printf("no se ingreso ninguno de los casos")

		
	}

	return &pb.HelloReply{Message: "Servidor NO  recibio la informacion correctamente."}, nil
	
	
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
