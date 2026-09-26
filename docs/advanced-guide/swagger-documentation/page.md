# Rendering OpenAPI Documentation in Pi

Pi supports automatic rendering of OpenAPI (also known as Swagger) documentation. This feature allows you to
easily provide interactive API documentation for your users.

## What is OpenAPI/Swagger Documentation?

OpenAPI, also known as Swagger, is a specification for building APIs. An OpenAPI file allows you to describe your entire API, including:

- Available endpoints (/users) and operations on each endpoint (GET /users, DELETE /users/{id})
- Operation parameters, input, and output for each operation
- Authentication methods
- Contact information, license, terms of use, and other information.

API specifications can be written in YAML or JSON. The format is easy to learn and readable to both humans and machines. 
The complete OpenAPI Specification can be found on the official [Swagger website](https://swagger.io/).

## Enabling Pi to render your openapi.json file

To allow Pi to render your OpenAPI documentation, simply place your `openapi.json` file inside the `static` directory of your project.
Pi will automatically render the Swagger documentation at the `/.well-known/swagger` endpoint.

Here are the steps:

- Create an `openapi.json` file that describes your API according to the OpenAPI specification.
- Place the `openapi.json` file inside the `static` directory in your project.
- Start your Pi server.
- Navigate to `/.well-known/swagger` on your server’s URL.

You should now see a beautifully rendered, interactive documentation for your API that users can use to understand and interact with your API.
