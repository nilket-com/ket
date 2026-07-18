//! Headless editor engine for Baryon.
//!
//! Frontends create an [`engine::EngineHandle`], send [`engine::EditorCommand`] values, and render
//! the [`uast::Viewport`] projections received from the handle's viewport channel. The engine has
//! no terminal dependencies; native, web, and terminal hosts share the same command/projection
//! protocol.

pub mod core;
mod ecs;
pub mod engine;
pub mod svp;
pub mod uast;
