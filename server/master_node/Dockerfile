#Master Node
FROM python:3
ENV TESTPORT=19199
ENV PORT=11223
ENV INITSH=dev_init
ENV HEART_BEAT_SEC=5
EXPOSE ${TESTPORT}
EXPOSE ${PORT}
ADD src/ /App/
RUN ./App/container_scripts/${INITSH}.sh
CMD [ "python", "/App/idle.py" ]