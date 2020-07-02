import com.ubirch.discovery.services.health.HealthChecks
import com.ubirch.discovery.Service
import javax.inject.{ Inject, Singleton }
import javax.servlet.ServletContext
import org.scalatra.LifeCycle

class ScalatraBootstrap extends LifeCycle {

  lazy val healthChecks = Service.get[HealthChecks]

  override def init(context: ServletContext): Unit = {
    context.mount(healthChecks, "/health", "HealthApi")
  }

}
