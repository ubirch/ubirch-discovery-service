import com.ubirch.discovery.rest.{APIJanusController, ApiSwagger, ResourcesApp}
import javax.servlet.ServletContext
import org.scalatra.LifeCycle

class ScalatraBootstrap extends LifeCycle {

  implicit val swagger: ApiSwagger = new ApiSwagger

  override def init(context: ServletContext) {
    context.mount(new APIJanusController, "/JG/*", "Janus")
    context.mount(new ResourcesApp, "/api-docs")
    context.initParameters("org.scalatra.cors.allowedOrigins") = "http://0.0.0.0"
  }
}

