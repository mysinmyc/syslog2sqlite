FROM golang as build

RUN go get github.com/mysinmyc/syslog2sqlite/cmd/syslog2sqlited

FROM alpine

RUN apk update \
        && apk add libc6-compat

RUN adduser -D syslog2sqlite

COPY --from=build  /go/bin/syslog2sqlited /usr/local/bin

RUN mkdir /var/opt/syslog2sqlite /var/opt/syslog2sqlite_archive

RUN chown syslog2sqlite /var/opt/syslog2sqlite /var/opt/syslog2sqlite_archive /usr/local/bin/syslog2sqlited

USER syslog2sqlite

EXPOSE 41514

VOLUME /var/opt/syslog2sqlite

ENTRYPOINT /usr/local/bin/syslog2sqlited --listen 0.0.0.0:41514 --db_path /var/opt/syslog2sqlite/syslog2sqlite.db --archive_dir /var/opt/syslog2sqlite_archive --archive_maxSizeMB 200 --archive_maxAgeMinutes 1440
