# Guía de Contribución

Bienvenido al proyecto de administración de servidores y bases de datos. Esta guía establece las normas y procesos para contribuir de manera efectiva, asegurando la calidad, la seguridad y el cumplimiento de estándares internacionales como ISO 27001 (control de cambios) y PMBOK (gestión de proyectos y calidad).

## Flujo de Trabajo Basado en Ramas

Utilizamos un modelo de ramas para gestionar el desarrollo y la producción:

- **Rama `develop`**: Destinada a pruebas y desarrollo. Aquí se integran todas las nuevas funcionalidades, correcciones y mejoras antes de pasar a producción.
- **Rama `master`**: Reservada exclusivamente para código en producción. Esta rama debe mantenerse estable y solo contener versiones validadas.

No se permiten cambios directos en la rama `master`. Todos los cambios deben iniciarse en una rama derivada de `develop`, ser probados exhaustivamente y luego integrarse mediante un Pull Request (PR).

## Proceso de Contribución

1. **Creación de una Rama de Trabajo**: Crea una nueva rama desde `develop` para tu tarea específica (ej. `feature/nueva-funcionalidad` o `bugfix/correccion-error`).
2. **Desarrollo y Pruebas**: Realiza tus cambios en la rama de trabajo. Asegúrate de que el código pase todas las pruebas locales y cumpla con los estándares de calidad.
3. **Pull Request**: Envía un PR desde tu rama de trabajo hacia `develop`. El PR debe incluir:
   - Una descripción clara de los cambios.
   - Referencias a issues relacionados.
   - Evidencia de pruebas realizadas.
4. **Validación en QA**: El equipo de QA revisará el PR en un ambiente de pruebas dedicado. Solo después de la aprobación se fusionará en `develop`.
5. **Liberación a Producción**: Las versiones estables de `develop` se fusionan en `master` mediante un proceso controlado, típicamente a través de tags de versión.

Este proceso asegura el control de cambios según ISO 27001, minimizando riesgos y facilitando la trazabilidad.

## Estándares de Mensajes de Commit

Los mensajes de commit deben ser claros, concisos y seguir el formato estándar para mantener la trazabilidad, especialmente para auditorías. Incluye el hostname de la VM afectada para contextualizar el cambio en entornos de servidores.

Formato recomendado:
```
[Tipo]: Descripción breve (hostname: nombre-del-host)

Descripción detallada si es necesario.
```

- **Tipos comunes**: `feat` (nueva funcionalidad), `fix` (corrección), `docs` (documentación), `refactor` (refactorización), `test` (pruebas).
- **Ejemplo**: `fix: Corregir error en consulta SQL (hostname: db-server-01)`

La inclusión del hostname facilita la auditoría y el seguimiento de cambios en entornos distribuidos, alineándose con los principios de gestión de configuración de PMBOK.

## Importancia de la Trazabilidad

La trazabilidad es fundamental para auditorías y cumplimiento normativo. Cada cambio debe estar vinculado a:
- Un issue o ticket en el sistema de seguimiento.
- Pruebas que validen el cambio.
- Documentación actualizada.

Esto asegura que podamos rastrear el origen, el impacto y la validación de cada modificación, cumpliendo con los requisitos de ISO 27001 para control de cambios y PMBOK para gestión de calidad y configuración.

## Cumplimiento Normativo

- **ISO 27001**: Los procesos descritos implementan controles de cambio seguros, con validación en QA y trazabilidad completa.
- **PMBOK**: La gestión de ramas y PRs refleja una PMO efectiva, con énfasis en calidad, riesgos y comunicación.

Si tienes dudas, consulta al equipo de liderazgo del proyecto.