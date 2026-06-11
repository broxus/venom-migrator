use std::future::Future;

pub fn spawn_background_task<F>(name: &'static str, fut: F)
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(err) = fut.await {
            tracing::error!(?err, name, "background task failed");
            panic!("background task failed: {name}: {err:?}");
        }
    });
}
