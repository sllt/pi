package main

import (
	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/http/response"
)

func main() {
	app := pi.New()
	app.GET("/list", listHandler)
	app.AddStaticFiles("/", "./static")
	app.Run()
}

type Todo struct {
	Title string
	Done  bool
}

type TodoPageData struct {
	PageTitle string
	Todos     []Todo
}

func listHandler(*pi.Context) (any, error) {
	// Get data from somewhere
	data := TodoPageData{
		PageTitle: "My TODO list",
		Todos: []Todo{
			{Title: "Expand on Pi documentation ", Done: false},
			{Title: "Add more examples", Done: true},
			{Title: "Write some articles", Done: false},
		},
	}

	return response.Template{Data: data, Name: "todo.html"}, nil
}
