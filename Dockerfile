FROM golang:1.26.0-alpine AS build
WORKDIR /app

COPY go.mod ./

COPY . .
RUN go build -o ti1s3 .

FROM alpine:3.22
WORKDIR /app
COPY --from=build /app/ti1s3 /app/ti1s3

CMD ["/app/ti1s3"]
