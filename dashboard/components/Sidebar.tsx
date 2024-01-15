"use client"

import { useState } from "react"
import {
    Home,
    FlaskConical,
    Github,
    Box,
    ChevronRight,
    ChevronLeft
  } from "lucide-react"
import { Nav } from '@/components/Nav'
import { Button } from '@/components/ui/button'


export default function Sidebar() {
  const [isCollapsed, setIsCollapsed] = useState(true)
  return (
    <div className="relative min-w-[80px] border-r px-3 pb-10 pt-24">
        <div className="absolute right-[-20px] top-8">
        <Button variant={"secondary"} className="rounded-full p-2">
            {isCollapsed ? (
                <ChevronRight
                className="h-6 w-6"
                onClick={() => setIsCollapsed(false)}
                />
            ) : (
                <ChevronLeft
                className="h-6 w-6"
                onClick={() => setIsCollapsed(true)}
                />
            )}
        </Button>
        </div>
        <Nav
            isCollapsed={isCollapsed}
            links={[
              {
                title: "dashboard",
                icon: Home,
                variant: "default",
                href: "/",
              },
              {
                title: "test config",
                icon: FlaskConical,
                variant: "default",
                href: "/test-config",
              },
              {
                title: "metrics",
                icon: Box,
                variant: "default",
                href: "/metrics",
              },
              {
                title: "about",
                icon: Github,
                variant: "default",
                href: "https://github.com/ankush-003"
              }
            ]}
          />
    </div>
  )
}